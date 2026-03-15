use std::collections::{HashMap, HashSet};
use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use geographiclib_rs::{Geodesic, InverseGeodesic};
use rusqlite::{params, Connection, OptionalExtension};

use crate::common::{opt_string_from_f64, row_opt_f64, row_opt_i64, row_opt_string, row_string, trimmed_float};

type SharedText = Arc<str>;

#[derive(Clone, Debug)]
struct Airport {
    id: i64,
    icao: SharedText,
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
    proc_code: String,
    name: SharedText,
    rwy: Option<SharedText>,
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
    icao: SharedText,
    proc_code: String,
    rwy: Option<SharedText>,
    terminal: SharedText,
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

#[derive(Clone)]
struct ProcedureListEntry {
    name: String,
    rwy: String,
    seq: usize,
}

#[derive(Default)]
struct ExistingProcedureList {
    entries: Vec<ProcedureListEntry>,
    map: HashMap<(String, String), usize>,
    procedures: HashMap<String, HashSet<String>>,
    next_seq: usize,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct TerminalFileKey {
    icao: String,
    proc_code: String,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct TerminalSectionKey {
    transition: String,
    via: String,
}

struct IndexedTerminalFileRows<'a> {
    section_keys: Vec<TerminalSectionKey>,
    ordered_rows: Vec<(usize, &'a MergedLeg)>,
}

struct TerminalWriteResult {
    files: Vec<PendingTerminalFile>,
}

struct PendingTerminalFile {
    file_key: TerminalFileKey,
    path: PathBuf,
    base_contents: String,
    copy_source: Option<PathBuf>,
    missing_detail_sections: Vec<usize>,
    must_write: bool,
}

enum PendingTerminalBuild {
    Skip,
    Pending(PendingTerminalFile),
}

enum CoordinateNameLookup<'a> {
    Single(&'a str),
    Multiple,
}

pub struct PreparedTerminalData {
    merged_data: Vec<MergedLeg>,
    list_rows: Vec<ListRow>,
    revision: String,
}

pub fn prepare(
    conn: &Connection,
    start_terminal_id: i64,
    end_terminal_id: i64,
) -> Result<PreparedTerminalData> {
    let merged_data = generate_merged_data(conn, start_terminal_id, end_terminal_id)?;
    let list_rows = build_terminal_list_rows(conn, &merged_data, start_terminal_id, end_terminal_id)?;
    let revision = get_revision_code_from_config()?;
    Ok(PreparedTerminalData {
        merged_data,
        list_rows,
        revision,
    })
}

pub fn write_prepared(prepared: &PreparedTerminalData, navdata_path: &Path) -> Result<()> {
    let permanent_path = navdata_path.join("Permanent");
    let supplemental_path = navdata_path.join("Supplemental");

    let merged_rows_by_file = group_merged_rows_by_file(&prepared.merged_data);

    let write_result = write_terminal_lists(
        &prepared.list_rows,
        &merged_rows_by_file,
        &permanent_path,
        navdata_path,
    )?;

    for pending_file in write_result.files {
        write_terminal_file(pending_file, &merged_rows_by_file)?;
    }

    crate::common::write_text_file(
        &supplemental_path.join("FMC_Ident.txt"),
        &format!("[Ident]\nSuppData=NAIP-{}\n", prepared.revision),
    )?;
    Ok(())
}

fn group_merged_rows_by_file<'a>(merged_rows: &'a [MergedLeg]) -> HashMap<TerminalFileKey, IndexedTerminalFileRows<'a>> {
    let mut grouped: HashMap<TerminalFileKey, IndexedTerminalFileRows<'a>> = HashMap::new();
    for row in merged_rows {
        let file_key = merged_leg_file_key(row);
        let section_key = merged_leg_section_key(row);
        let indexed = grouped.entry(file_key).or_insert_with(|| IndexedTerminalFileRows {
            section_keys: Vec::new(),
            ordered_rows: Vec::new(),
        });
        let section_index = if let Some(section_index) = indexed.section_keys.iter().position(|candidate| candidate == &section_key) {
            section_index
        } else {
            indexed.section_keys.push(section_key);
            indexed.section_keys.len() - 1
        };
        indexed.ordered_rows.push((section_index, row));
    }
    grouped
}

fn generate_merged_data(
    conn: &Connection,
    start_terminal_id: i64,
    end_terminal_id: i64,
) -> Result<Vec<MergedLeg>> {
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
    let navaid_by_id: HashMap<i64, Navaid> = navaids.into_iter().collect();
    let runways_by_airport_and_ident: HashMap<(i64, String), Runway> = runways
        .into_iter()
        .map(|runway| ((runway.airport_id, runway.ident.clone()), runway))
        .collect();

    let mut merged_rows = Vec::with_capacity(terminal_legs.len());
    for leg in terminal_legs {
        let TerminalLegRow {
            id,
            terminal_id,
            type_code,
            transition,
            track_code,
            wpt_id,
            wpt_lat,
            wpt_lon,
            turn_dir,
            nav_id,
            nav_bear,
            nav_dist,
            course,
            distance,
            alt,
            vnav,
            center_id,
        } = leg;

        let Some(terminal) = terminal_by_id.get(&terminal_id) else {
            continue;
        };
        let Some(airport) = airport_by_id.get(&terminal.airport_id) else {
            continue;
        };
        let ex = terminal_leg_ex_by_id.get(&id);
        let waypoint = wpt_id.and_then(|id| waypoints.get(&id));
        let navaid = nav_id.and_then(|id| navaid_by_id.get(&id));
        let center = center_id.and_then(|id| waypoints.get(&id));
        let speed = build_speed_limit(ex);
        let cross_this_point = build_cross_this_point(ex);

        merged_rows.push(MergedLeg {
            airport_id: airport.id,
            icao: airport.icao.clone(),
            proc_code: terminal.proc_code.clone(),
            rwy: terminal.rwy.clone(),
            terminal: terminal.name.clone(),
            type_code,
            transition,
            leg: track_code,
            turn_direction: turn_dir,
            name: waypoint.map(|value| value.ident.clone()),
            latitude: wpt_lat,
            longitude: wpt_lon,
            frequency: navaid.map(|value| value.ident.clone()),
            nav_bear,
            nav_dist,
            heading: course,
            dist: distance,
            cross_this_point,
            altitude: alt,
            map: None,
            slope: vnav,
            speed,
            center_lat: center.map(|value| value.latitude),
            center_lon: center.map(|value| value.longitude),
        });
    }
    apply_map_logic(&mut merged_rows, &waypoints, &runways_by_airport_and_ident)?;

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
            icao: row.get::<_, String>(1)?.into(),
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
        let proc_code = row_string(row, 2)?;
        let _icao = row_string(row, 3)?;
        Ok(TerminalDef {
            id: row.get(0)?,
            airport_id: row.get(1)?,
            proc_code,
            name: row_string(row, 4)?.into(),
            rwy: row_opt_string(row, 5)?.map(Into::into),
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
    waypoints: &HashMap<i64, Waypoint>,
    runways_by_airport_and_ident: &HashMap<(i64, String), Runway>,
) -> Result<()> {
    let geodesic = Geodesic::wgs84();
    let waypoint_by_coordinates = build_waypoint_coordinate_lookup(waypoints);
    for index in 0..merged_rows.len() {
        if merged_rows[index].altitude.as_deref() != Some("MAP") {
            continue;
        }

        let previous_context = merged_rows[..index].iter().rev().find_map(|row| {
            let altitude = row.altitude.as_deref()?;
            if altitude.is_empty() {
                return None;
            }
            Some((
                parse_altitude_value(altitude),
                row.latitude.zip(row.longitude),
            ))
        });
        let row = &mut merged_rows[index];
        row.map = Some(1);

        let runway_ident = row.rwy.as_deref().map(|value| normalize_runway_value(Some(value)));
        let runway = runway_ident
            .as_ref()
            .map(|ident| (row.airport_id, ident.clone()))
            .as_ref()
            .and_then(|key| runways_by_airport_and_ident.get(key));

        if let (Some(latitude), Some(longitude)) = (row.latitude, row.longitude) {
            if let Some(name_lookup) = waypoint_by_coordinates.get(&(latitude.to_bits(), longitude.to_bits())) {
                match name_lookup {
                    CoordinateNameLookup::Single(name) => row.name = Some((*name).to_string()),
                    CoordinateNameLookup::Multiple if row.name.is_none() => {
                        if let Some(runway) = runway {
                            row.latitude = Some(runway.latitude);
                            row.longitude = Some(runway.longitude);
                            row.name = Some(build_runway_ident(&row.terminal));
                        }
                    }
                    CoordinateNameLookup::Multiple => {}
                }
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
        let Some((Some(previous_altitude), Some((previous_lat, previous_lon)))) = previous_context else {
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

fn build_waypoint_coordinate_lookup(
    waypoints: &HashMap<i64, Waypoint>,
) -> HashMap<(u64, u64), CoordinateNameLookup<'_>> {
    let mut grouped = HashMap::new();
    for waypoint in waypoints.values() {
        let key = (waypoint.latitude.to_bits(), waypoint.longitude.to_bits());
        match grouped.entry(key) {
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(CoordinateNameLookup::Single(waypoint.ident.as_str()));
            }
            std::collections::hash_map::Entry::Occupied(mut entry) => match entry.get() {
                CoordinateNameLookup::Single(existing) if *existing == waypoint.ident.as_str() => {}
                CoordinateNameLookup::Single(_) => {
                    entry.insert(CoordinateNameLookup::Multiple);
                }
                CoordinateNameLookup::Multiple => {}
            },
        }
    }
    grouped
}

fn parse_altitude_value(value: &str) -> Option<f64> {
    let mut parsed = 0f64;
    let mut has_digit = false;
    for digit in value.bytes().filter(u8::is_ascii_digit) {
        parsed = parsed * 10.0 + f64::from(digit - b'0');
        has_digit = true;
    }
    has_digit.then_some(parsed)
}

fn apply_terminal_post_processing(merged_rows: &mut Vec<MergedLeg>) {
    let runway_transitions = build_transition_runway_lookup(merged_rows);

    for row in merged_rows.iter_mut() {
        if row.rwy.is_none() && row.transition.starts_with("RW") {
            row.rwy = Some(row.transition[2..].to_string().into());
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
        .filter(|row| !(row.transition == "ALL" && row.rwy.is_none()))
        .cloned()
        .collect();

    for (_, row) in rows_to_process {
        let Some(runways) = runway_transitions
            .get(row.icao.as_ref())
            .and_then(|terminals| terminals.get(row.terminal.as_ref()))
        else {
            continue;
        };
        for rwy in runways {
            let mut cloned = row.clone();
            cloned.rwy = Some(rwy.clone().into());
            cloned.type_code = "5".to_string();
            expanded_rows.push(cloned);
        }
    }

    for row in expanded_rows.iter_mut() {
        if row.leg.as_deref() == Some("IF") && row.name.is_none() {
            if let Some(rwy) = row.rwy.as_ref() {
                row.name = Some(format!("RW{}", normalize_runway_value(Some(rwy.as_ref()))));
            }
        }
    }

    expanded_rows.sort_by(|left, right| {
        left.icao
            .as_ref()
            .cmp(right.icao.as_ref())
            .then(left.terminal.as_ref().cmp(right.terminal.as_ref()))
            .then(left.rwy.cmp(&right.rwy))
    });
    *merged_rows = expanded_rows;
}

fn build_transition_runway_lookup(merged_rows: &[MergedLeg]) -> HashMap<String, HashMap<String, Vec<String>>> {
    let mut grouped = HashMap::new();
    for row in merged_rows {
        if !row.transition.starts_with("RW") {
            continue;
        }
        push_unique_nested_value(
            &mut grouped,
            row.icao.as_ref(),
            row.terminal.as_ref(),
            row.transition[2..].to_string(),
        );
    }
    grouped
}

fn build_terminal_runway_lookup(merged_rows: &[MergedLeg]) -> HashMap<String, HashMap<String, Vec<String>>> {
    let mut grouped = HashMap::new();
    for row in merged_rows {
        let Some(rwy) = row.rwy.as_ref() else {
            continue;
        };
        push_unique_nested_value(&mut grouped, row.icao.as_ref(), row.terminal.as_ref(), rwy.to_string());
    }
    grouped
}

fn push_unique_nested_value(
    grouped: &mut HashMap<String, HashMap<String, Vec<String>>>,
    left: &str,
    right: &str,
    value: String,
) {
    let values = grouped
        .entry(left.to_string())
        .or_default()
        .entry(right.to_string())
        .or_default();
    if !values.iter().any(|existing| existing == &value) {
        values.push(value);
    }
}

fn write_terminal_lists(
    list_rows: &[ListRow],
    merged_rows_by_file: &HashMap<TerminalFileKey, IndexedTerminalFileRows<'_>>,
    permanent_path: &Path,
    navdata_path: &Path,
) -> Result<TerminalWriteResult> {
    let supplemental_sid = navdata_path.join("Supplemental").join("Sid");
    let supplemental_star = navdata_path.join("Supplemental").join("Star");
    fs::create_dir_all(&supplemental_sid)?;
    fs::create_dir_all(&supplemental_star)?;

    let mut grouped: HashMap<(String, String), Vec<ListRow>> = HashMap::new();
    for row in list_rows.iter().cloned() {
        grouped.entry((row.icao.clone(), row.proc_code.clone())).or_default().push(row);
    }

    let mut files = Vec::new();
    for ((icao, proc_code), rows) in grouped {
        match write_list_file(
            &icao,
            &proc_code,
            &rows,
            merged_rows_by_file,
            permanent_path,
            navdata_path,
        )? {
            PendingTerminalBuild::Skip => {}
            PendingTerminalBuild::Pending(file_state) => files.push(file_state),
        }
    }

    files.sort_by(|left, right| left.path.cmp(&right.path));
    Ok(TerminalWriteResult { files })
}

fn build_terminal_list_rows(
    conn: &Connection,
    merged_rows: &[MergedLeg],
    start_terminal_id: i64,
    end_terminal_id: i64,
) -> Result<Vec<ListRow>> {
    let runway_lookup = build_terminal_runway_lookup(merged_rows);
    let mut rows = Vec::new();
    for row in merged_rows {
        if matches!(row.type_code.as_str(), "6" | "A") {
            rows.push(ListRow {
                proc_code: row.type_code.clone(),
                icao: row.icao.to_string(),
                name: row.transition.clone(),
                rwy: Some(row.terminal.to_string()),
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
        let Some(rwys) = runway_lookup
            .get(row.icao.as_str())
            .and_then(|names| names.get(row.name.as_str()))
        else {
            processed.push(row);
            continue;
        };
        if rwys.is_empty() {
            processed.push(row);
            continue;
        }
        let ListRow {
            proc_code,
            icao,
            name,
            rwy: _,
        } = row;
        for rwy in rwys {
            processed.push(ListRow {
                proc_code: proc_code.clone(),
                icao: icao.clone(),
                name: name.clone(),
                rwy: Some(rwy.clone()),
            });
        }
    }

    Ok(processed)
}

fn write_list_file(
    icao: &str,
    proc_code: &str,
    rows: &[ListRow],
    merged_rows_by_file: &HashMap<TerminalFileKey, IndexedTerminalFileRows<'_>>,
    permanent_path: &Path,
    navdata_path: &Path,
) -> Result<PendingTerminalBuild> {
    let Some(file_name) = terminal_output_path(icao, proc_code, navdata_path) else {
        return Ok(PendingTerminalBuild::Skip);
    };
    let supplemental_root = navdata_path.join("Supplemental");
    let output_exists = file_name.exists();
    let seed_file = load_seed_terminal_file_contents(
        permanent_path,
        &supplemental_root,
        &file_name,
    )?;
    let file_key = TerminalFileKey {
        icao: icao.to_string(),
        proc_code: proc_code.to_string(),
    };
    let mut parsed_seed = parse_terminal_file_contents(&file_name, &seed_file.contents);
    let mut missing_list_count = 0usize;
    for row in rows {
        let runway = zfill_runway_value(row.rwy.as_deref());
        if parsed_seed.existing.map.contains_key(&(row.name.clone(), runway.clone())) {
            continue;
        }
        missing_list_count += 1;
        let seq = parsed_seed.existing.next_seq;
        parsed_seed.existing.next_seq += 1;
        parsed_seed.existing.entries.push(ProcedureListEntry {
            name: row.name.clone(),
            rwy: runway.clone(),
            seq,
        });
        parsed_seed
            .existing
            .map
            .insert((row.name.clone(), runway.clone()), seq);
        insert_procedure_lookup(&mut parsed_seed.existing.procedures, &row.name, &runway);
    }

    let missing_detail_sections = collect_missing_detail_sections(
        &parsed_seed.existing.procedures,
        &parsed_seed.details,
        merged_rows_by_file.get(&file_key),
    );

    let base_contents = if missing_list_count > 0 {
        build_list_file_contents(&seed_file.contents, &parsed_seed.existing.entries)
    } else {
        seed_file.contents
    };

    let copy_source = if missing_list_count == 0 {
        seed_file.copy_source
    } else {
        None
    };

    if output_exists && missing_list_count == 0 && missing_detail_sections.is_empty() {
        return Ok(PendingTerminalBuild::Skip);
    }

    Ok(PendingTerminalBuild::Pending(PendingTerminalFile {
        file_key,
        path: file_name,
        base_contents,
        copy_source,
        missing_detail_sections,
        must_write: missing_list_count > 0 || !output_exists,
    }))
}

fn terminal_output_path(icao: &str, proc_code: &str, navdata_path: &Path) -> Option<PathBuf> {
    match proc_code {
        "2" => Some(navdata_path.join("Supplemental").join("Sid").join(format!("{icao}.sid"))),
        "1" => Some(navdata_path.join("Supplemental").join("Star").join(format!("{icao}.star"))),
        "3" => Some(navdata_path.join("Supplemental").join("Star").join(format!("{icao}.app"))),
        "6" => Some(navdata_path.join("Supplemental").join("Sid").join(format!("{icao}.sidtrs"))),
        "A" => Some(navdata_path.join("Supplemental").join("Star").join(format!("{icao}.apptrs"))),
        _ => None,
    }
}

fn load_seed_terminal_file_contents(
    permanent_path: &Path,
    supplemental_root: &Path,
    file_path: &Path,
) -> Result<SeedTerminalFileContents> {
    if file_path.exists() {
        return Ok(SeedTerminalFileContents {
            contents: fs::read_to_string(file_path)
                .with_context(|| format!("无法读取 {}", file_path.display()))?,
            copy_source: None,
        });
    }

    let relative = file_path
        .strip_prefix(supplemental_root)
        .with_context(|| format!("无法计算相对路径: {}", file_path.display()))?;
    let source_path = resolve_seed_source_path(permanent_path, relative);
    if !source_path.exists() {
        return Ok(SeedTerminalFileContents {
            contents: String::new(),
            copy_source: None,
        });
    }

    Ok(SeedTerminalFileContents {
        contents: fs::read_to_string(&source_path)
            .with_context(|| format!("无法读取 {}", source_path.display()))?,
        copy_source: Some(source_path),
    })
}

fn resolve_seed_source_path(permanent_path: &Path, relative: &Path) -> PathBuf {
    let direct_path = permanent_path.join(relative);
    if direct_path.exists() {
        return direct_path;
    }

    let mut normalized = PathBuf::new();
    for (index, component) in relative.components().enumerate() {
        let part = component.as_os_str().to_string_lossy();
        let adjusted = if index == 0 {
            match part.as_ref() {
                "Sid" => "SID",
                "Star" => "STAR",
                "Supp" => "SUPP",
                _ => part.as_ref(),
            }
        } else {
            part.as_ref()
        };
        normalized.push(adjusted);
    }

    permanent_path.join(normalized)
}

struct ParsedTerminalFile {
    existing: ExistingProcedureList,
    details: HashMap<String, HashSet<String>>,
}

struct SeedTerminalFileContents {
    contents: String,
    copy_source: Option<PathBuf>,
}

fn write_terminal_file(
    file_state: PendingTerminalFile,
    merged_rows_by_file: &HashMap<TerminalFileKey, IndexedTerminalFileRows<'_>>,
) -> Result<()> {
    let generated_sections = generate_missing_leg_sections(
        &file_state.file_key,
        &file_state.missing_detail_sections,
        merged_rows_by_file,
    );

    if generated_sections.is_empty() && !file_state.must_write {
        return Ok(());
    }

    if generated_sections.is_empty() {
        if let Some(copy_source) = file_state.copy_source.as_ref() {
            if let Some(parent) = file_state.path.parent() {
                fs::create_dir_all(parent)?;
            }
            let source_contents = fs::read_to_string(copy_source)
                .with_context(|| format!("无法读取 {}", copy_source.display()))?;
            crate::common::write_text_file(&file_state.path, &source_contents)?;
            return Ok(());
        }
    }

    let mut output = file_state.base_contents;
    if !generated_sections.is_empty() {
        let trailing_newlines = output.bytes().rev().take_while(|byte| *byte == b'\n').count();
        if !output.is_empty() && trailing_newlines < 2 {
            for _ in trailing_newlines..2 {
                output.push('\n');
            }
        }
        output.push_str(&generated_sections);
    }
    if !output.ends_with('\n') {
        output.push('\n');
    }
    if let Some(parent) = file_state.path.parent() {
        fs::create_dir_all(parent)?;
    }
    crate::common::write_text_file(&file_state.path, &output)?;
    Ok(())
}

fn parse_terminal_file_contents(file_path: &Path, contents: &str) -> ParsedTerminalFile {
    let extension = file_path.extension().and_then(|ext| ext.to_str()).unwrap_or_default();
    let should_parse_details = matches!(extension, "app" | "apptrs" | "sid" | "sidtrs" | "star");

    let mut existing = ExistingProcedureList::default();
    let mut details = HashMap::new();
    let mut max_seq = 0usize;
    let mut in_list_section = false;

    for line in contents.lines() {
        if line.starts_with("[list]") {
            in_list_section = true;
            continue;
        }
        if line.starts_with('[') {
            in_list_section = false;
        }
        if in_list_section {
            if let Some((seq, name, rwy)) = parse_procedure_line(line) {
                existing.entries.push(ProcedureListEntry {
                    name: name.to_string(),
                    rwy: rwy.to_string(),
                    seq,
                });
                existing.map.insert((name.to_string(), rwy.to_string()), seq);
                insert_procedure_lookup(&mut existing.procedures, name, rwy);
                max_seq = max_seq.max(seq);
            }
            continue;
        }
        if should_parse_details {
            if let Some((name, rwy)) = parse_detail_line(line) {
                insert_lookup_value(&mut details, name, rwy);
            }
        }
    }

    existing.next_seq = max_seq + 1;

    ParsedTerminalFile {
        existing,
        details,
    }
}

fn build_list_file_contents(existing_contents: &str, entries: &[ProcedureListEntry]) -> String {
    let detail_start = existing_contents
        .lines()
        .enumerate()
        .skip(1)
        .find_map(|(index, line)| line.starts_with('[').then_some(index))
        .and_then(|index| nth_line_byte_index(existing_contents, index));

    let suffix = detail_start.map(|start| &existing_contents[start..]).unwrap_or_default();
    let mut output = String::with_capacity(existing_contents.len() + entries.len().saturating_mul(24));
    output.push_str("[list]\n");
    for entry in entries {
        let _ = writeln!(&mut output, "Procedure.{}={}.{}", entry.seq, entry.name, entry.rwy);
    }
    output.push_str(suffix.trim_start_matches('\n'));
    output
}

fn nth_line_byte_index(contents: &str, target_index: usize) -> Option<usize> {
    if target_index == 0 {
        return Some(0);
    }
    let mut line_index = 0usize;
    for (byte_index, ch) in contents.char_indices() {
        if ch == '\n' {
            line_index += 1;
            if line_index == target_index {
                return Some(byte_index + ch.len_utf8());
            }
        }
    }
    None
}

fn build_speed_limit(ex: Option<&TerminalLegExRow>) -> Option<String> {
    let value = ex?;
    match (
        value.speed_limit.as_deref(),
        value.speed_limit_description.as_deref(),
    ) {
        (Some(limit), Some(description)) => {
            let mut merged = String::with_capacity(limit.len() + description.len());
            merged.push_str(limit);
            merged.push_str(description);
            Some(merged)
        }
        (Some(limit), None) => Some(limit.to_string()),
        (None, Some(description)) => Some(description.to_string()),
        (None, None) => None,
    }
}

fn build_cross_this_point(ex: Option<&TerminalLegExRow>) -> Option<String> {
    match ex.and_then(|value| value.is_fly_over) {
        Some(0) => None,
        Some(value) => Some(format!("{:.1}", value as f64)),
        None => Some("nan".to_string()),
    }
}

fn parse_procedure_line(line: &str) -> Option<(usize, &str, &str)> {
    let payload = line.strip_prefix("Procedure.")?;
    let (seq, name_rwy) = payload.split_once('=')?;
    let (name, rwy) = name_rwy.rsplit_once('.')?;
    if name.is_empty() || rwy.is_empty() || name.contains(char::is_whitespace) || rwy.contains(char::is_whitespace) {
        return None;
    }
    Some((seq.parse().ok()?, name, rwy))
}

fn parse_detail_line(line: &str) -> Option<(&str, &str)> {
    let payload = line.strip_prefix('[')?.strip_suffix(']')?;
    let mut parts = payload.split('.');
    let name = parts.next()?;
    let rwy = parts.next()?;
    parts.next()?;
    if name.is_empty() || rwy.is_empty() || name.contains(char::is_whitespace) || rwy.contains(char::is_whitespace) {
        return None;
    }
    Some((name, rwy))
}

fn merged_leg_file_key(row: &MergedLeg) -> TerminalFileKey {
    let proc_code = if matches!(row.type_code.as_str(), "6" | "A") {
        row.type_code.clone()
    } else {
        row.proc_code.clone()
    };
    TerminalFileKey {
        icao: row.icao.to_string(),
        proc_code,
    }
}

fn merged_leg_section_key(row: &MergedLeg) -> TerminalSectionKey {
    if matches!(row.type_code.as_str(), "6" | "A") {
        TerminalSectionKey {
            transition: row.transition.clone(),
            via: row.terminal.to_string(),
        }
    } else {
        TerminalSectionKey {
            transition: row.terminal.to_string(),
            via: zfill_runway_value(row.rwy.as_deref()),
        }
    }
}

fn generate_missing_leg_sections(
    file_key: &TerminalFileKey,
    missing_detail_sections: &[usize],
    merged_rows_by_file: &HashMap<TerminalFileKey, IndexedTerminalFileRows<'_>>,
) -> String {
    let Some(indexed_rows) = merged_rows_by_file.get(file_key) else {
        return String::new();
    };
    let missing_section_set: HashSet<usize> = missing_detail_sections.iter().copied().collect();
    let mut output = String::new();
    let mut current_transition = String::new();
    let mut current_via = String::new();
    let mut seq = 0usize;
    let mut wrote_section = false;

    for (section_index, row) in &indexed_rows.ordered_rows {
        if !missing_section_set.contains(section_index) {
            continue;
        }
        let section_key = &indexed_rows.section_keys[*section_index];
        if current_transition != section_key.transition || current_via != section_key.via {
            current_transition.clear();
            current_transition.push_str(&section_key.transition);
            current_via.clear();
            current_via.push_str(&section_key.via);
            seq = 0;
        } else {
            seq += 1;
        }

        if wrote_section {
            output.push('\n');
        }
        wrote_section = true;

        let _ = writeln!(&mut output, "[{}.{}.{}]", section_key.transition, section_key.via, seq);
        append_leg_field(&mut output, "Leg", row.leg.as_deref());
        append_leg_field(&mut output, "TurnDirection", row.turn_direction.as_deref());
        append_leg_field(&mut output, "Name", row.name.as_deref());

        let latitude = opt_string_from_f64(row.latitude);
        append_leg_field(&mut output, "Latitude", latitude.as_deref());
        let longitude = opt_string_from_f64(row.longitude);
        append_leg_field(&mut output, "Longitude", longitude.as_deref());

        append_leg_field(&mut output, "Frequency", row.frequency.as_deref());
        append_leg_field(&mut output, "NavBear", row.nav_bear.as_deref());
        append_leg_field(&mut output, "NavDist", row.nav_dist.as_deref());
        append_leg_field(&mut output, "Heading", row.heading.as_deref());
        append_leg_field(&mut output, "Dist", row.dist.as_deref());
        append_leg_field(&mut output, "CrossThisPoint", row.cross_this_point.as_deref());
        append_leg_field(&mut output, "Altitude", row.altitude.as_deref());

        let map = row.map.map(|value| value.to_string());
        append_leg_field(&mut output, "MAP", map.as_deref());
        let slope = row.slope.map(trimmed_float);
        append_leg_field(&mut output, "Slope", slope.as_deref());
        append_leg_field(&mut output, "Speed", row.speed.as_deref());

        let center_lat = opt_string_from_f64(row.center_lat);
        append_leg_field(&mut output, "CenterLat", center_lat.as_deref());
        let center_lon = opt_string_from_f64(row.center_lon);
        append_leg_field(&mut output, "CenterLon", center_lon.as_deref());

        if output.ends_with('\n') {
            output.pop();
        }
    }

    output
}

fn collect_missing_detail_sections(
    procedures: &HashMap<String, HashSet<String>>,
    details: &HashMap<String, HashSet<String>>,
    indexed_rows: Option<&IndexedTerminalFileRows<'_>>,
) -> Vec<usize> {
    let Some(indexed_rows) = indexed_rows else {
        return Vec::new();
    };
    let mut missing_sections = Vec::new();
    for (section_index, section_key) in indexed_rows.section_keys.iter().enumerate() {
        if !lookup_contains(procedures, &section_key.transition, &section_key.via) {
            continue;
        }
        if lookup_contains(details, &section_key.transition, &section_key.via) {
            continue;
        }
        missing_sections.push(section_index);
    }
    missing_sections
}

fn insert_lookup_value(grouped: &mut HashMap<String, HashSet<String>>, left: &str, right: &str) {
    grouped
        .entry(left.to_string())
        .or_default()
        .insert(right.to_string());
}

fn insert_procedure_lookup(grouped: &mut HashMap<String, HashSet<String>>, name: &str, rwy: &str) {
    if name.is_empty() || rwy.is_empty() {
        return;
    }
    insert_lookup_value(grouped, name, rwy);
}

fn lookup_contains(grouped: &HashMap<String, HashSet<String>>, left: &str, right: &str) -> bool {
    grouped.get(left).is_some_and(|values| values.contains(right))
}

fn append_leg_field(section: &mut String, key: &str, value: Option<&str>) {
    if let Some(value) = value.filter(|value| !value.is_empty()) {
        let _ = writeln!(section, "{key}={value}");
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

fn normalize_runway_value(raw_value: Option<&str>) -> String {
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

fn zfill_runway_value(raw_value: Option<&str>) -> String {
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
