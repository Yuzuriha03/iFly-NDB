use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use geographiclib_rs::{Geodesic, InverseGeodesic};
use regex::Regex;
use rusqlite::{params, Connection, OptionalExtension};
use walkdir::WalkDir;

use crate::common::{opt_string_from_f64, row_opt_f64, row_opt_i64, row_opt_string, row_string, trimmed_float};

#[derive(Clone, Debug)]
struct Airport {
    id: i64,
    icao: String,
}

#[derive(Clone, Debug)]
struct Runway {
    airport_id: i64,
    ident: String,
    latitude: f64,
    longitude: f64,
    elevation: Option<f64>,
}

#[derive(Clone, Debug)]
struct TerminalDef {
    id: i64,
    airport_id: i64,
    icao: String,
    name: String,
    rwy: Option<String>,
}

#[derive(Clone, Debug)]
struct TerminalLegRow {
    id: i64,
    terminal_id: i64,
    type_code: String,
    transition: String,
    track_code: Option<String>,
    wpt_id: Option<i64>,
    wpt_lat: Option<f64>,
    wpt_lon: Option<f64>,
    turn_dir: Option<String>,
    nav_id: Option<i64>,
    nav_bear: Option<String>,
    nav_dist: Option<String>,
    course: Option<String>,
    distance: Option<String>,
    alt: Option<String>,
    vnav: Option<f64>,
    center_id: Option<i64>,
}

#[derive(Clone, Debug)]
struct TerminalLegExRow {
    id: i64,
    is_fly_over: Option<i64>,
    speed_limit: Option<String>,
    speed_limit_description: Option<String>,
}

#[derive(Clone, Debug)]
struct Waypoint {
    ident: String,
    latitude: f64,
    longitude: f64,
}

#[derive(Clone, Debug)]
struct Navaid {
    ident: String,
}

#[derive(Clone, Debug)]
struct MergedLeg {
    airport_id: i64,
    icao: String,
    rwy: Option<String>,
    terminal: String,
    type_code: String,
    transition: String,
    leg: Option<String>,
    turn_direction: Option<String>,
    name: Option<String>,
    latitude: Option<f64>,
    longitude: Option<f64>,
    frequency: Option<String>,
    nav_bear: Option<String>,
    nav_dist: Option<String>,
    heading: Option<String>,
    dist: Option<String>,
    cross_this_point: Option<String>,
    altitude: Option<String>,
    map: Option<i64>,
    slope: Option<f64>,
    speed: Option<String>,
    center_lat: Option<f64>,
    center_lon: Option<f64>,
}

#[derive(Clone, Debug)]
struct ListRow {
    proc_code: String,
    icao: String,
    name: String,
    rwy: Option<String>,
}

#[derive(Default)]
struct ExistingProcedureList {
    entries: Vec<(String, usize)>,
    map: HashMap<String, usize>,
    next_seq: usize,
}

pub fn run(conn: &Connection, navdata_path: &Path, start_terminal_id: i64, end_terminal_id: i64) -> Result<()> {
    let started = std::time::Instant::now();
    let permanent_path = navdata_path.join("Permanent");
    let supplemental_path = navdata_path.join("Supplemental");

    copy_existing_terminal_files(&permanent_path, &supplemental_path)?;

    let merged_data = generate_merged_data(conn, start_terminal_id, end_terminal_id)?;
    write_terminal_lists(conn, &merged_data, start_terminal_id, end_terminal_id, navdata_path)?;

    for entry in WalkDir::new(&supplemental_path).into_iter().filter_map(Result::ok) {
        if entry.file_type().is_file() {
            process_terminal_file(entry.path(), &merged_data)?;
        }
    }

    let revision = get_revision_code_from_config()?;
    fs::write(
        supplemental_path.join("FMC_Ident.txt"),
        format!("[Ident]\nSuppData=NAIP-{revision}\n"),
    )?;

    println!("终端数据转换完毕，用时：{:.3}秒", started.elapsed().as_secs_f64());
    Ok(())
}

fn copy_existing_terminal_files(permanent_path: &Path, supplemental_path: &Path) -> Result<()> {
    let allowed_prefixes = ["OPGT", "VHHX", "ZB", "ZG", "ZH", "ZJ", "ZL", "ZP", "ZS", "ZU", "ZW", "ZY"];
    let allowed_extensions = ["sid", "sidtrs", "app", "apptrs", "star", "startrs"];

    for entry in WalkDir::new(permanent_path).into_iter().filter_map(Result::ok) {
        if !entry.file_type().is_file() {
            continue;
        }
        let file_name = entry.file_name().to_string_lossy();
        let extension = entry.path().extension().and_then(|ext| ext.to_str()).unwrap_or_default();
        if !allowed_extensions.contains(&extension) {
            continue;
        }
        if !allowed_prefixes.iter().any(|prefix| file_name.starts_with(prefix)) {
            continue;
        }

        let relative = entry
            .path()
            .strip_prefix(permanent_path)
            .with_context(|| format!("无法计算相对路径: {}", entry.path().display()))?;
        let destination = supplemental_path.join(relative);
        if destination.exists() {
            continue;
        }
        if let Some(parent) = destination.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::copy(entry.path(), &destination).with_context(|| {
            format!("无法复制 {} -> {}", entry.path().display(), destination.display())
        })?;
    }

    Ok(())
}

fn generate_merged_data(conn: &Connection, start_terminal_id: i64, end_terminal_id: i64) -> Result<Vec<MergedLeg>> {
    let airports = load_airports(conn)?;
    if airports.is_empty() {
        return Ok(Vec::new());
    }
    let airport_ids: Vec<i64> = airports.iter().map(|airport| airport.id).collect();
    let airport_by_id: HashMap<i64, Airport> = airports.into_iter().map(|airport| (airport.id, airport)).collect();

    let runways = load_runways(conn, &airport_ids)?;
    let terminals = load_terminals(conn, &airport_ids, start_terminal_id, end_terminal_id)?;
    if terminals.is_empty() {
        return Ok(Vec::new());
    }
    let terminal_ids: Vec<i64> = terminals.iter().map(|terminal| terminal.id).collect();
    let terminal_by_id: HashMap<i64, TerminalDef> = terminals.iter().cloned().map(|terminal| (terminal.id, terminal)).collect();

    let terminal_legs = load_terminal_legs(conn, &terminal_ids)?;
    let terminal_leg_ids: Vec<i64> = terminal_legs.iter().map(|leg| leg.id).collect();
    let terminal_legs_ex = load_terminal_legs_ex(conn, &terminal_leg_ids)?;
    let terminal_leg_ex_by_id: HashMap<i64, TerminalLegExRow> = terminal_legs_ex.into_iter().map(|row| (row.id, row)).collect();

    let nav_ids: Vec<i64> = terminal_legs.iter().filter_map(|leg| leg.nav_id).collect();
    let waypoints = load_all_waypoints(conn)?;
    let navaids = load_navaids(conn, &nav_ids)?;
    let waypoint_by_id: HashMap<i64, Waypoint> = waypoints.clone().into_iter().collect();
    let navaid_by_id: HashMap<i64, Navaid> = navaids.into_iter().collect();
    let runways_by_airport_and_ident: HashMap<(i64, String), Runway> = runways
        .into_iter()
        .map(|runway| ((runway.airport_id, runway.ident.clone()), runway))
        .collect();

    let mut merged_rows = Vec::new();
    for leg in terminal_legs {
        let Some(terminal) = terminal_by_id.get(&leg.terminal_id) else {
            continue;
        };
        let Some(airport) = airport_by_id.get(&terminal.airport_id) else {
            continue;
        };
        let ex = terminal_leg_ex_by_id.get(&leg.id);
        let waypoint = leg.wpt_id.and_then(|id| waypoint_by_id.get(&id));
        let navaid = leg.nav_id.and_then(|id| navaid_by_id.get(&id));
        let center = leg.center_id.and_then(|id| waypoint_by_id.get(&id));

        let speed = match (
            ex.and_then(|value| value.speed_limit.clone()),
            ex.and_then(|value| value.speed_limit_description.clone()),
        ) {
            (Some(limit), Some(description)) => Some(format!("{limit}{description}")),
            (Some(limit), None) => Some(limit),
            (None, Some(description)) => Some(description),
            (None, None) => None,
        };

        let cross_this_point = match ex.and_then(|value| value.is_fly_over) {
            Some(0) => None,
            Some(value) => Some(format!("{:.1}", value as f64)),
            None => Some("nan".to_string()),
        };

        merged_rows.push(MergedLeg {
            airport_id: airport.id,
            icao: airport.icao.clone(),
            rwy: terminal.rwy.clone(),
            terminal: terminal.name.clone(),
            type_code: leg.type_code.clone(),
            transition: leg.transition.clone(),
            leg: leg.track_code.clone(),
            turn_direction: leg.turn_dir.clone(),
            name: waypoint.map(|value| value.ident.clone()),
            latitude: leg.wpt_lat,
            longitude: leg.wpt_lon,
            frequency: navaid.map(|value| value.ident.clone()),
            nav_bear: leg.nav_bear.clone(),
            nav_dist: leg.nav_dist.clone(),
            heading: leg.course.clone(),
            dist: leg.distance.clone(),
            cross_this_point,
            altitude: leg.alt.clone(),
            map: None,
            slope: leg.vnav,
            speed,
            center_lat: center.map(|value| value.latitude),
            center_lon: center.map(|value| value.longitude),
        });
    }

    apply_map_logic(&mut merged_rows, &terminals, &waypoints, &runways_by_airport_and_ident)?;
    apply_terminal_post_processing(&mut merged_rows);
    Ok(merged_rows)
}

fn load_airports(conn: &Connection) -> Result<Vec<Airport>> {
    let mut stmt = conn.prepare(
        "SELECT ID, ICAO FROM Airports WHERE ICAO IN ('OPGT', 'VHHX') OR SUBSTR(ICAO, 1, 2) IN ('ZB', 'ZG', 'ZH', 'ZJ', 'ZL', 'ZP', 'ZS', 'ZU', 'ZW', 'ZY')",
    )?;
    let rows = stmt.query_map([], |row| {
        Ok(Airport {
            id: row.get(0)?,
            icao: row.get(1)?,
        })
    })?;
    Ok(rows.collect::<rusqlite::Result<_>>()?)
}

fn load_runways(conn: &Connection, airport_ids: &[i64]) -> Result<Vec<Runway>> {
    let query = format!(
        "SELECT AirportID, Ident, Latitude, Longtitude, Elevation FROM Runways WHERE AirportID IN ({})",
        join_i64_values(airport_ids)
    );
    let mut stmt = conn.prepare(&query)?;
    let rows = stmt.query_map([], |row| {
        Ok(Runway {
            airport_id: row.get(0)?,
            ident: row_string(row, 1)?,
            latitude: row_opt_f64(row, 2)?.unwrap_or_default(),
            longitude: row_opt_f64(row, 3)?.unwrap_or_default(),
            elevation: row_opt_f64(row, 4)?,
        })
    })?;
    Ok(rows.collect::<rusqlite::Result<_>>()?)
}

fn load_terminals(conn: &Connection, airport_ids: &[i64], start_terminal_id: i64, end_terminal_id: i64) -> Result<Vec<TerminalDef>> {
    let query = format!(
        "SELECT ID, AirportID, Proc, ICAO, Name, Rwy FROM Terminals WHERE ID BETWEEN ? AND ? AND AirportID IN ({})",
        join_i64_values(airport_ids)
    );
    let mut stmt = conn.prepare(&query)?;
    let rows = stmt.query_map(params![start_terminal_id, end_terminal_id], |row| {
        let _proc_code = row_string(row, 2)?;
        Ok(TerminalDef {
            id: row.get(0)?,
            airport_id: row.get(1)?,
            icao: row_string(row, 3)?,
            name: row_string(row, 4)?,
            rwy: row_opt_string(row, 5)?,
        })
    })?;
    Ok(rows.collect::<rusqlite::Result<_>>()?)
}

fn load_terminal_legs(conn: &Connection, terminal_ids: &[i64]) -> Result<Vec<TerminalLegRow>> {
    let query = format!(
        "SELECT ID, TerminalID, Type, Transition, TrackCode, WptID, WptLat, WptLon, TurnDir, NavID, NavBear, NavDist, Course, Distance, Alt, Vnav, CenterID FROM TerminalLegs WHERE TerminalID IN ({})",
        join_i64_values(terminal_ids)
    );
    let mut stmt = conn.prepare(&query)?;
    let rows = stmt.query_map([], |row| {
        Ok(TerminalLegRow {
            id: row.get(0)?,
            terminal_id: row.get(1)?,
            type_code: row_string(row, 2)?,
            transition: row_string(row, 3)?,
            track_code: row_opt_string(row, 4)?,
            wpt_id: row_opt_i64(row, 5)?,
            wpt_lat: row_opt_f64(row, 6)?,
            wpt_lon: row_opt_f64(row, 7)?,
            turn_dir: row_opt_string(row, 8)?,
            nav_id: row_opt_i64(row, 9)?,
            nav_bear: row_opt_string(row, 10)?,
            nav_dist: row_opt_string(row, 11)?,
            course: row_opt_string(row, 12)?,
            distance: row_opt_string(row, 13)?,
            alt: row_opt_string(row, 14)?,
            vnav: row_opt_f64(row, 15)?,
            center_id: row_opt_i64(row, 16)?,
        })
    })?;
    Ok(rows.collect::<rusqlite::Result<_>>()?)
}

fn load_terminal_legs_ex(conn: &Connection, terminal_leg_ids: &[i64]) -> Result<Vec<TerminalLegExRow>> {
    let query = format!(
        "SELECT ID, IsFlyOver, SpeedLimit, SpeedLimitDescription FROM TerminalLegsEx WHERE ID IN ({})",
        join_i64_values(terminal_leg_ids)
    );
    let mut stmt = conn.prepare(&query)?;
    let rows = stmt.query_map([], |row| {
        Ok(TerminalLegExRow {
            id: row.get(0)?,
            is_fly_over: row_opt_i64(row, 1)?,
            speed_limit: row_opt_i64(row, 2)?.map(|value| value.to_string()),
            speed_limit_description: row_opt_string(row, 3)?,
        })
    })?;
    Ok(rows.collect::<rusqlite::Result<_>>()?)
}

fn load_all_waypoints(conn: &Connection) -> Result<HashMap<i64, Waypoint>> {
    let mut stmt = conn.prepare("SELECT ID, Ident, Latitude, Longtitude FROM Waypoints")?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            Waypoint {
                ident: row.get(1)?,
                latitude: row.get(2)?,
                longitude: row.get(3)?,
            },
        ))
    })?;
    Ok(rows.collect::<rusqlite::Result<_>>()?)
}

fn load_navaids(conn: &Connection, nav_ids: &[i64]) -> Result<HashMap<i64, Navaid>> {
    if nav_ids.is_empty() {
        return Ok(HashMap::new());
    }
    let query = format!(
        "SELECT ID, Ident FROM Navaids WHERE ID IN ({})",
        join_i64_values(nav_ids)
    );
    let mut stmt = conn.prepare(&query)?;
    let rows = stmt.query_map([], |row| {
        Ok((row.get::<_, i64>(0)?, Navaid { ident: row.get(1)? }))
    })?;
    Ok(rows.collect::<rusqlite::Result<_>>()?)
}

fn apply_map_logic(
    merged_rows: &mut [MergedLeg],
    terminals: &[TerminalDef],
    waypoints: &HashMap<i64, Waypoint>,
    runways_by_airport_and_ident: &HashMap<(i64, String), Runway>,
) -> Result<()> {
    let geodesic = Geodesic::wgs84();
    let waypoint_by_coordinates: HashMap<(u64, u64), String> = waypoints
        .values()
        .map(|waypoint| ((waypoint.latitude.to_bits(), waypoint.longitude.to_bits()), waypoint.ident.clone()))
        .collect();
    let terminals_by_icao: HashMap<&str, Vec<&TerminalDef>> = {
        let mut map: HashMap<&str, Vec<&TerminalDef>> = HashMap::new();
        for terminal in terminals {
            map.entry(terminal.icao.as_str()).or_default().push(terminal);
        }
        map
    };

    let map_indices: Vec<usize> = merged_rows
        .iter()
        .enumerate()
        .filter_map(|(index, row)| (row.altitude.as_deref() == Some("MAP")).then_some(index))
        .collect();

    for index in map_indices {
        let previous_altitude_index = (0..index)
            .rev()
            .find(|candidate| {
                merged_rows[*candidate]
                    .altitude
                    .as_deref()
                    .is_some_and(|value| !value.is_empty())
            });

        let previous_altitude_text = previous_altitude_index.and_then(|previous_index| {
            merged_rows[previous_index].altitude.clone()
        });
        let previous_coordinates = previous_altitude_index.and_then(|previous_index| {
            match (merged_rows[previous_index].latitude, merged_rows[previous_index].longitude) {
                (Some(latitude), Some(longitude)) => Some((latitude, longitude)),
                _ => None,
            }
        });

        let row = &mut merged_rows[index];
        row.map = Some(1);

        let rwy = row.rwy.clone().map(|value| normalize_runway_value(Some(value)));
        let runway_key = rwy.clone().map(|ident| (row.airport_id, ident));
        let runway = runway_key
            .as_ref()
            .and_then(|key| runways_by_airport_and_ident.get(key));

        if let (Some(latitude), Some(longitude)) = (row.latitude, row.longitude) {
            if let Some(name) = waypoint_by_coordinates.get(&(latitude.to_bits(), longitude.to_bits())) {
                row.name = Some(name.clone());
            } else if let Some(runway) = runway {
                row.latitude = Some(runway.latitude);
                row.longitude = Some(runway.longitude);
                row.name = Some(build_runway_ident(&row.terminal));
            }
        } else if let Some(runway) = runway {
            row.latitude = Some(runway.latitude);
            row.longitude = Some(runway.longitude);
            row.name = Some(build_runway_ident(&row.terminal));
        }

        let runway_elevation = runway.and_then(|value| value.elevation);
        let slope_value = row.slope;
        let Some(_previous_index) = previous_altitude_index else {
            continue;
        };
        let Some(previous_altitude_text) = previous_altitude_text else {
            continue;
        };
        let previous_altitude_digits: String = previous_altitude_text.chars().filter(|c| c.is_ascii_digit()).collect();
        if previous_altitude_digits.is_empty() {
            continue;
        }
        let previous_altitude: f64 = previous_altitude_digits.parse()?;
        let Some((previous_lat, previous_lon)) = previous_coordinates else {
            continue;
        };
        let (Some(current_lat), Some(current_lon)) = (row.latitude, row.longitude) else {
            continue;
        };
        let (Some(runway_elevation), Some(slope_value)) = (runway_elevation, slope_value) else {
            continue;
        };

        let distance_m: f64 = geodesic.inverse(previous_lat, previous_lon, current_lat, current_lon);
        let distance_ft = distance_m / 0.3048;
        let altitude = previous_altitude - distance_ft * slope_value.to_radians().tan();
        let fallback = runway_elevation.round() + 50.0;
        let chosen_altitude = if runway_elevation + 50.0 <= altitude && altitude < 16_000.0 {
            altitude.round()
        } else {
            fallback
        };
        row.altitude = Some(chosen_altitude.round().to_string());

        let _ = terminals_by_icao.get(row.icao.as_str());
    }

    for row in merged_rows.iter_mut() {
        if let Some(name) = row.name.as_deref() {
            row.name = Some(match name {
                "ZJ400" => "RW15".to_string(),
                "HJ600" => "RW06".to_string(),
                "QT800" => "RW27".to_string(),
                "RQ610" => "RW04".to_string(),
                "SC600" => "RW33".to_string(),
                "TK800" => "RW33".to_string(),
                _ => name.to_string(),
            });
        }
    }

    Ok(())
}

fn apply_terminal_post_processing(merged_rows: &mut Vec<MergedLeg>) {
    for row in merged_rows.iter_mut() {
        if row.rwy.is_none() && row.transition.starts_with("RW") {
            row.rwy = Some(row.transition[2..].to_string());
            row.type_code = "5".to_string();
        }
    }

    let rows_to_process: Vec<(usize, MergedLeg)> = merged_rows
        .iter()
        .cloned()
        .enumerate()
        .filter(|(_, row)| row.transition == "ALL" && row.rwy.is_none())
        .collect();

    let mut expanded_rows: Vec<MergedLeg> = merged_rows
        .iter()
        .cloned()
        .filter(|row| !(row.transition == "ALL" && row.rwy.is_none()))
        .collect();

    for (index, row) in rows_to_process {
        let mut transitions = Vec::new();
        for (candidate_index, candidate) in merged_rows.iter().enumerate() {
            if candidate_index == index {
                continue;
            }
            if candidate.icao == row.icao
                && candidate.terminal == row.terminal
                && !candidate.transition.is_empty()
                && !transitions.contains(&candidate.transition)
            {
                transitions.push(candidate.transition.clone());
            }
        }

        for rwy in transitions
            .into_iter()
            .filter(|transition| transition.starts_with("RW"))
            .map(|transition| transition[2..].to_string())
        {
            let mut cloned = row.clone();
            cloned.rwy = Some(rwy);
            cloned.type_code = "5".to_string();
            expanded_rows.push(cloned);
        }
    }

    for row in expanded_rows.iter_mut() {
        if row.leg.as_deref() == Some("IF") && row.name.is_none() {
            if let Some(rwy) = row.rwy.as_ref() {
                row.name = Some(format!("RW{}", normalize_runway_value(Some(rwy.clone()))));
            }
        }
    }

    expanded_rows.sort_by(|left, right| {
        left.icao
            .cmp(&right.icao)
            .then(left.terminal.cmp(&right.terminal))
            .then(left.rwy.cmp(&right.rwy))
    });
    *merged_rows = expanded_rows;
}

fn write_terminal_lists(
    conn: &Connection,
    merged_rows: &[MergedLeg],
    start_terminal_id: i64,
    end_terminal_id: i64,
    navdata_path: &Path,
) -> Result<()> {
    let list_rows = build_terminal_list_rows(conn, merged_rows, start_terminal_id, end_terminal_id)?;
    let supplemental_sid = navdata_path.join("Supplemental").join("SID");
    let supplemental_star = navdata_path.join("Supplemental").join("STAR");
    fs::create_dir_all(&supplemental_sid)?;
    fs::create_dir_all(&supplemental_star)?;

    let mut grouped: HashMap<(String, String), Vec<ListRow>> = HashMap::new();
    for row in list_rows {
        grouped.entry((row.icao.clone(), row.proc_code.clone())).or_default().push(row);
    }

    for ((icao, proc_code), rows) in grouped {
        write_list_file(&icao, &proc_code, &rows, navdata_path)?;
    }

    Ok(())
}

fn build_terminal_list_rows(
    conn: &Connection,
    merged_rows: &[MergedLeg],
    start_terminal_id: i64,
    end_terminal_id: i64,
) -> Result<Vec<ListRow>> {
    let mut rows = Vec::new();
    for row in merged_rows {
        if matches!(row.type_code.as_str(), "6" | "A") {
            rows.push(ListRow {
                proc_code: row.type_code.clone(),
                icao: row.icao.clone(),
                name: row.transition.clone(),
                rwy: Some(row.terminal.clone()),
            });
        }
    }

    let mut stmt = conn.prepare(
        "SELECT Proc, ICAO, Name, Rwy FROM Terminals WHERE ID BETWEEN ? AND ?",
    )?;
    let terminal_rows = stmt.query_map(params![start_terminal_id, end_terminal_id], |row| {
        Ok(ListRow {
            proc_code: row_string(row, 0)?,
            icao: row_string(row, 1)?,
            name: row_string(row, 2)?,
            rwy: row_opt_string(row, 3)?,
        })
    })?;

    let mut terminal_rows = terminal_rows.collect::<rusqlite::Result<Vec<_>>>()?;
    terminal_rows.retain(|row| !row.icao.chars().any(|c| c.is_ascii_digit()));
    rows.extend(terminal_rows);

    let mut processed = Vec::new();
    for row in rows {
        if row.rwy.is_some() {
            processed.push(row);
            continue;
        }
        let mut rwys = Vec::new();
        for merged in merged_rows
            .iter()
            .filter(|merged| merged.icao == row.icao && merged.terminal == row.name)
        {
            if let Some(rwy) = merged.rwy.clone() {
                if !rwys.contains(&rwy) {
                    rwys.push(rwy);
                }
            }
        }
        if rwys.is_empty() {
            processed.push(row);
        } else {
            for rwy in rwys {
                let mut cloned = row.clone();
                cloned.rwy = Some(rwy);
                processed.push(cloned);
            }
        }
    }

    Ok(processed)
}

fn write_list_file(icao: &str, proc_code: &str, rows: &[ListRow], navdata_path: &Path) -> Result<()> {
    let file_name = match proc_code {
        "2" => navdata_path.join("Supplemental").join("SID").join(format!("{icao}.sid")),
        "1" => navdata_path.join("Supplemental").join("STAR").join(format!("{icao}.star")),
        "3" => navdata_path.join("Supplemental").join("STAR").join(format!("{icao}.app")),
        "6" => navdata_path.join("Supplemental").join("SID").join(format!("{icao}.sidtrs")),
        "A" => navdata_path.join("Supplemental").join("STAR").join(format!("{icao}.apptrs")),
        _ => return Ok(()),
    };

    if let Some(parent) = file_name.parent() {
        fs::create_dir_all(parent)?;
    }
    if !file_name.exists() {
        fs::write(&file_name, "")?;
    }

    let mut existing = parse_existing_file(&file_name)?;
    for row in rows {
        let name_rwy = format!("{}.{}", row.name, zfill_runway_value(row.rwy.clone()));
        if existing.map.contains_key(&name_rwy) {
            continue;
        }
        let seq = existing.next_seq;
        existing.next_seq += 1;
        existing.map.insert(name_rwy.clone(), seq);
        existing.entries.push((name_rwy, seq));
    }

    let mut lines = fs::read_to_string(&file_name)?.lines().map(|line| format!("{line}\n")).collect::<Vec<_>>();
    let second_bracket = lines.iter().enumerate().skip(1).find_map(|(index, line)| line.starts_with('[').then_some(index));
    if let Some(index) = second_bracket {
        lines = lines[index..].to_vec();
    }
    lines.insert(0, "[list]\n".to_string());

    let new_lines = existing
        .entries
        .iter()
        .map(|(name_rwy, seq)| {
            let mut parts = name_rwy.split('.');
            let name = parts.next().unwrap_or_default();
            let rwy = parts.next().unwrap_or_default();
            format!("Procedure.{seq}={name}.{rwy}\n")
        })
        .collect::<Vec<_>>();

    lines.splice(1..1, new_lines);
    if let Some(last) = lines.last_mut() {
        *last = last.trim_end_matches('\n').to_string();
    }
    fs::write(&file_name, lines.concat())?;
    Ok(())
}

fn parse_existing_file(file_path: &Path) -> Result<ExistingProcedureList> {
    if !file_path.exists() {
        return Ok(ExistingProcedureList {
            next_seq: 1,
            ..ExistingProcedureList::default()
        });
    }
    let contents = fs::read_to_string(file_path)?;
    let procedure_re = Regex::new(r"Procedure\.(\d+)=(\S+)\.(\S+)")?;

    let mut parsed = ExistingProcedureList::default();
    let mut max_seq = 0usize;
    for line in contents.lines() {
        if line.starts_with("[list]") {
            continue;
        }
        if line.starts_with('[') {
            break;
        }
        if let Some(caps) = procedure_re.captures(line) {
            let seq: usize = caps.get(1).unwrap().as_str().parse().unwrap_or_default();
            let name_rwy = format!("{}.{}", &caps[2], &caps[3]);
            parsed.map.insert(name_rwy.clone(), seq);
            parsed.entries.push((name_rwy, seq));
            max_seq = max_seq.max(seq);
        }
    }
    parsed.next_seq = max_seq + 1;
    Ok(parsed)
}

fn process_terminal_file(file_path: &Path, merged_rows: &[MergedLeg]) -> Result<()> {
    let procedures = parse_procedures(file_path)?;
    let details = parse_detail_names(file_path)?;
    let icao = file_path.file_stem().and_then(|stem| stem.to_str()).unwrap_or_default();
    let results = generate_leg_sections(icao, &procedures, &details, merged_rows);

    let mut lines = fs::read_to_string(file_path)?.lines().map(|line| line.to_string()).collect::<Vec<_>>();
    lines.push(String::new());
    lines.extend(results);
    fs::write(file_path, lines.join("\n") + "\n")?;
    Ok(())
}

fn parse_procedures(file_path: &Path) -> Result<HashSet<String>> {
    let extension = file_path.extension().and_then(|ext| ext.to_str()).unwrap_or_default();
    if !matches!(extension, "app" | "apptrs" | "sid" | "sidtrs" | "star") {
        return Ok(HashSet::new());
    }

    let icao = file_path.file_stem().and_then(|stem| stem.to_str()).unwrap_or_default();
    let contents = fs::read_to_string(file_path)?;
    let re = Regex::new(r"Procedure\.(\d+)=(\S+)\.(\S+)")?;
    let mut started = false;
    let mut procedures = HashSet::new();

    for line in contents.lines() {
        if line.starts_with("[list]") {
            started = true;
            continue;
        }
        if started && line.starts_with('[') {
            break;
        }
        if started {
            if let Some(caps) = re.captures(line) {
                procedures.insert(format!("{icao}.{}.{}", &caps[2], &caps[3]));
            }
        }
    }

    Ok(procedures)
}

fn parse_detail_names(file_path: &Path) -> Result<HashSet<String>> {
    let contents = fs::read_to_string(file_path)?;
    let re = Regex::new(r"\[(\S+)\.(\S+)\.(\d+)\]")?;
    let mut details = HashSet::new();
    for line in contents.lines() {
        if let Some(caps) = re.captures(line) {
            details.insert(format!("{}.{}", &caps[1], &caps[2]));
        }
    }
    Ok(details)
}

fn generate_leg_sections(
    icao: &str,
    procedures: &HashSet<String>,
    details: &HashSet<String>,
    merged_rows: &[MergedLeg],
) -> Vec<String> {
    let mut results = Vec::new();
    let mut current_transition: Option<String> = None;
    let mut current_via: Option<String> = None;
    let mut seq = 0usize;

    for row in merged_rows.iter().filter(|row| row.icao == icao) {
        let (transition, via) = if matches!(row.type_code.as_str(), "6" | "A") {
            (row.transition.clone(), row.terminal.clone())
        } else {
            (row.terminal.clone(), zfill_runway_value(row.rwy.clone()))
        };

        let procedure_name = format!("{}.{}.{}", row.icao, transition, via);
        let detail_name = format!("{}.{}", transition, via);
        if !procedures.contains(&procedure_name) || details.contains(&detail_name) {
            continue;
        }

        if current_transition.as_deref() != Some(transition.as_str()) || current_via.as_deref() != Some(via.as_str()) {
            current_transition = Some(transition.clone());
            current_via = Some(via.clone());
            seq = 0;
        } else {
            seq += 1;
        }

        let mut lines = vec![format!("[{}.{}.{}]", transition, via, seq)];
        append_leg_field(&mut lines, "Leg", row.leg.clone());
        append_leg_field(&mut lines, "TurnDirection", row.turn_direction.clone());
        append_leg_field(&mut lines, "Name", row.name.clone());
        append_leg_field(&mut lines, "Latitude", opt_string_from_f64(row.latitude));
        append_leg_field(&mut lines, "Longitude", opt_string_from_f64(row.longitude));
        append_leg_field(&mut lines, "Frequency", row.frequency.clone());
        append_leg_field(&mut lines, "NavBear", row.nav_bear.clone());
        append_leg_field(&mut lines, "NavDist", row.nav_dist.clone());
        append_leg_field(&mut lines, "Heading", row.heading.clone());
        append_leg_field(&mut lines, "Dist", row.dist.clone());
        append_leg_field(&mut lines, "CrossThisPoint", row.cross_this_point.clone());
        append_leg_field(&mut lines, "Altitude", row.altitude.clone());
        append_leg_field(&mut lines, "MAP", row.map.map(|value| value.to_string()));
        append_leg_field(&mut lines, "Slope", row.slope.map(trimmed_float));
        append_leg_field(&mut lines, "Speed", row.speed.clone());
        append_leg_field(&mut lines, "CenterLat", opt_string_from_f64(row.center_lat));
        append_leg_field(&mut lines, "CenterLon", opt_string_from_f64(row.center_lon));
        results.push(lines.join("\n"));
    }

    results
}

fn append_leg_field(lines: &mut Vec<String>, key: &str, value: Option<String>) {
    if let Some(value) = value {
        if !value.is_empty() {
            lines.push(format!("{key}={value}"));
        }
    }
}

fn get_revision_code_from_config() -> Result<String> {
    let db_path = PathBuf::from(r"C:\ProgramData\Fenix\Navdata\nd.db3");
    if !db_path.exists() {
        return Ok("None".to_string());
    }

    let row = (|| -> Result<Option<String>> {
        let conn = Connection::open(&db_path)
            .with_context(|| format!("无法连接配置数据库: {}", db_path.display()))?;
        let value = conn.query_row(
            "SELECT val FROM config WHERE key='CycleName' LIMIT 1",
            [],
            |row| row_opt_string(row, 0),
        )
        .optional()?;
        Ok(value.flatten())
    })();

    match row {
        Ok(Some(value)) => Ok(value.trim().to_string()),
        Ok(None) => Ok("2601".to_string()),
        Err(_) => Ok("None".to_string()),
    }
}

fn join_i64_values(values: &[i64]) -> String {
    values.iter().map(i64::to_string).collect::<Vec<_>>().join(", ")
}

fn normalize_runway_value(raw_value: Option<String>) -> String {
    let Some(raw_value) = raw_value else {
        return "".to_string();
    };
    let raw_value = raw_value.trim().to_uppercase();
    if raw_value.is_empty() {
        return raw_value;
    }
    if let Ok(value) = raw_value.parse::<f64>() {
        return format!("{:02}", value as i64);
    }
    let digits: String = raw_value.chars().filter(|c| c.is_ascii_digit()).collect();
    if digits.is_empty() {
        raw_value
    } else {
        format!("{:0>2}", digits)
    }
}

fn zfill_runway_value(raw_value: Option<String>) -> String {
    let Some(raw_value) = raw_value else {
        return "None".to_string();
    };
    let raw_value = raw_value.trim().to_string();
    if raw_value.len() >= 2 {
        raw_value
    } else {
        format!("{:0>2}", raw_value)
    }
}

fn build_runway_ident(terminal_value: &str) -> String {
    if terminal_value.len() >= 4 {
        let ident = terminal_value[1..4].replace('-', "");
        if !ident.is_empty() {
            return format!("RW{ident}");
        }
    }
    "RWXX".to_string()
}
