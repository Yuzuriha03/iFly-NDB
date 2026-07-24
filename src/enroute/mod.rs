pub mod route;

use std::collections::HashMap;
use std::fmt::Write as _;
use std::fs;
use std::path::Path;

use anyhow::{Context, Result};
use num_traits::ToPrimitive;
use rusqlite::{params, Connection};

use crate::common::{row_opt_f64, row_opt_i64, row_opt_string, row_string};
use crate::geomag::magnetic;

#[derive(Clone)]
struct RunwayTask {
    airport_name: String,
    icao: String,
    ident: String,
    length: f64,
    latitude: f64,
    longitude: f64,
    true_heading: f64,
    frequency: String,
    elevation: f64,
}

struct NavaidRow {
    ident: String,
    type_code: i64,
    name: String,
    freq: i64,
    usage: String,
    latitude: f64,
    longitude: f64,
}

pub struct PreparedEnrouteData {
    airport_data: String,
    supp_files: Vec<(String, String)>,
    wpnavapt_data: Option<String>,
    wpnavaid_data: Option<String>,
    wpnavfix_data: Option<String>,
    route_data: route::PreparedRouteData,
}

pub fn prepare(conn: &Connection, csv_path: &Path) -> Result<Option<PreparedEnrouteData>> {
    let Some(start_airport_id) = start_airport_id(conn)? else {
        return Ok(None);
    };

    Ok(Some(PreparedEnrouteData {
        airport_data: build_airport_data(conn, start_airport_id)?,
        supp_files: build_supp_files(conn, start_airport_id)?,
        wpnavapt_data: build_wpnavapt_data(conn, start_airport_id)?,
        wpnavaid_data: build_wpnavaid_data(conn)?,
        wpnavfix_data: build_wpnavfix_data(conn)?,
        route_data: route::prepare_wpnavrte(conn, csv_path)?,
    }))
}

pub fn write_prepared(
    prepared: &PreparedEnrouteData,
    route_file: &Path,
    navdata_path: &Path,
) -> Result<()> {
    write_airport_data(navdata_path, &prepared.airport_data)?;
    write_supp_files(navdata_path, &prepared.supp_files)?;
    write_optional_supplemental_file(
        navdata_path,
        "WPNAVAPT.txt",
        prepared.wpnavapt_data.as_deref(),
    )?;
    write_optional_supplemental_file(
        navdata_path,
        "WPNAVAID.txt",
        prepared.wpnavaid_data.as_deref(),
    )?;
    write_optional_supplemental_file(
        navdata_path,
        "WPNAVFIX.txt",
        prepared.wpnavfix_data.as_deref(),
    )?;

    let generated_route_file = route::write_prepared_wpnavrte(&prepared.route_data, navdata_path)?;
    let checked_routes = route::check_route(route_file, &generated_route_file)?;
    route::insert_and_order_route(route_file, &generated_route_file, checked_routes)?;
    Ok(())
}

fn start_airport_id(conn: &Connection) -> Result<Option<i64>> {
    let start_id = conn.query_row("SELECT ID FROM airports WHERE ICAO = 'ZYYJ'", [], |row| {
        row.get::<_, i64>(0)
    });

    match start_id {
        Ok(id) => Ok(Some(id + 1)),
        Err(rusqlite::Error::QueryReturnedNoRows) => {
            println!("未找到对应的机场。");
            Ok(None)
        }
        Err(error) => Err(error).context("查询起始机场失败"),
    }
}

fn build_airport_data(conn: &Connection, start_id: i64) -> Result<String> {
    let mut stmt = conn.prepare(
        "SELECT ICAO, Latitude, Longtitude FROM airports WHERE ID >= ? ORDER BY Latitude ASC",
    )?;
    let rows = stmt.query_map(params![start_id], |row| {
        let icao: String = row.get(0)?;
        let latitude: f64 = row.get(1)?;
        let longitude: f64 = row.get(2)?;
        Ok((latitude, format!("{icao}{latitude:>10.6}{longitude:>11.6}")))
    })?;

    let mut lines = Vec::new();
    for row in rows {
        lines.push(row?.1);
    }
    if lines.is_empty() {
        Ok(String::new())
    } else {
        Ok(lines.join("\n") + "\n")
    }
}

fn write_airport_data(navdata_path: &Path, contents: &str) -> Result<()> {
    write_optional_supplemental_file(navdata_path, "AIRPORTS.dat", Some(contents))
}

fn build_supp_files(conn: &Connection, start_airport_id: i64) -> Result<Vec<(String, String)>> {
    let mut stmt = conn.prepare(
        "SELECT ICAO, TransitionAltitude, TransitionLevel, SpeedLimit FROM airports WHERE ID >= ?",
    )?;
    let rows = stmt.query_map(params![start_airport_id], |row| {
        Ok((
            row.get::<_, String>(0)?,
            normalized_supp_value(row_opt_string(row, 1)?, "18000"),
            normalized_supp_value(row_opt_string(row, 2)?, "18000"),
            normalized_supp_value(row_opt_string(row, 3)?, "250"),
        ))
    })?;

    let mut files = Vec::new();
    for row in rows {
        let (icao, transition_altitude, transition_level, speed_limit) = row?;
        let file_contents = [
            "[Speed_Transition]".to_string(),
            format!("Speed={speed_limit}"),
            "Altitude=10000".to_string(),
            String::new(),
            "[Transition_Altitude]".to_string(),
            format!("Altitude={transition_altitude}"),
            String::new(),
            "[Transition_Level]".to_string(),
            format!("Altitude={transition_level}"),
        ];
        files.push((format!("{icao}.supp"), file_contents.join("\n")));
    }

    Ok(files)
}

fn normalized_supp_value(value: Option<String>, default: &str) -> String {
    value
        .filter(|value| value.trim().parse::<f64>().is_ok_and(|number| number > 0.0))
        .unwrap_or_else(|| default.to_string())
}

fn write_supp_files(navdata_path: &Path, supp_files: &[(String, String)]) -> Result<()> {
    let output_folder = navdata_path.join("Supplemental").join("Supp");
    fs::create_dir_all(&output_folder)?;
    for (file_name, contents) in supp_files {
        crate::common::write_text_file(&output_folder.join(file_name), contents)?;
    }
    Ok(())
}

fn build_wpnavapt_data(conn: &Connection, start_airport_id: i64) -> Result<Option<String>> {
    let ils_frequency_cache = load_ils_frequency_cache(conn)?;
    let mut stmt = conn.prepare(
        "SELECT a.Name, a.ICAO, r.ID, r.Ident, r.TrueHeading, r.Length, r.Latitude, r.Longtitude, r.Elevation \
         FROM airports a \
         JOIN runways r ON r.AirportID = a.ID \
         WHERE a.ID >= ?",
    )?;
    let runway_rows = stmt.query_map(params![start_airport_id], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, i64>(2)?,
            row.get::<_, String>(3)?,
            row_opt_f64(row, 4)?.unwrap_or_default(),
            row_opt_f64(row, 5)?.unwrap_or_default(),
            row_opt_f64(row, 6)?.unwrap_or_default(),
            row_opt_f64(row, 7)?.unwrap_or_default(),
            row_opt_f64(row, 8)?.unwrap_or_default(),
        ))
    })?;

    let mut tasks = Vec::new();
    for runway_row in runway_rows {
        let (
            airport_name,
            icao,
            runway_id,
            ident,
            true_heading,
            length,
            latitude,
            longitude,
            elevation,
        ) = runway_row?;
        let frequency = ils_frequency_cache
            .get(&runway_id)
            .cloned()
            .unwrap_or_else(|| "000.00".to_string());
        tasks.push(RunwayTask {
            airport_name,
            icao,
            ident,
            length,
            latitude,
            longitude,
            true_heading,
            frequency,
            elevation,
        });
    }

    if tasks.is_empty() {
        println!("未找到需要处理的跑道数据");
        return Ok(None);
    }

    tasks.sort_by(|left, right| {
        left.icao
            .cmp(&right.icao)
            .then(left.ident.cmp(&right.ident))
    });

    let declinations = magnetic::batch_get_magnetic_variations(
        &tasks
            .iter()
            .map(|task| (task.latitude, task.longitude))
            .collect::<Vec<_>>(),
    )?;

    let mut body = String::with_capacity(tasks.len().saturating_mul(72));
    for (task, declination) in tasks.iter().zip(declinations) {
        append_wpnavapt_row(&mut body, task, declination);
    }
    Ok(Some(body))
}

fn append_wpnavapt_row(buffer: &mut String, task: &RunwayTask, declination: f64) {
    let magnetic_heading = (task.true_heading - declination)
        .round()
        .to_i64()
        .unwrap_or_default();
    let runway_length = task.length.round().to_i64().unwrap_or_default();
    let runway_elevation = task.elevation.round().to_i64().unwrap_or_default();
    let _ = writeln!(
        buffer,
        "{:<24}{}{: <3}{:05}{:03}{:>10.6}{:>11.6}{}{:03}{:05}",
        fixed_width_text(&task.airport_name, 24),
        task.icao,
        task.ident,
        runway_length,
        magnetic_heading,
        task.latitude,
        task.longitude,
        task.frequency,
        magnetic_heading,
        runway_elevation,
    );
}

fn load_ils_frequency_cache(conn: &Connection) -> Result<HashMap<i64, String>> {
    let mut stmt = conn.prepare(
        "SELECT t.RwyID, i.Freq \
         FROM terminals t \
         JOIN ILSes i ON i.ID = t.ilsID \
         WHERE t.RwyID IS NOT NULL AND t.ilsID IS NOT NULL",
    )?;
    let mut cache = HashMap::new();
    let rows = stmt.query_map([], |row| Ok((row.get::<_, i64>(0)?, row_opt_i64(row, 1)?)))?;
    for row in rows {
        let (runway_id, freq_value) = row?;
        if cache.contains_key(&runway_id) {
            continue;
        }
        let frequency = freq_value.map_or_else(|| "000.00".to_string(), format_ils_frequency);
        cache.insert(runway_id, frequency);
    }
    Ok(cache)
}

fn format_ils_frequency(freq: i64) -> String {
    let packed_digits = format!("{freq:X}").parse::<f64>().unwrap_or_default();
    format!("{:.2}", packed_digits / 10_000.0)
}

fn build_wpnavaid_data(conn: &Connection) -> Result<Option<String>> {
    let start_id: i64 = match conn.query_row(
        "SELECT ID FROM navaids WHERE Name = 'DEXIN YANJI'",
        [],
        |row| row.get(0),
    ) {
        Ok(start_id) => start_id,
        Err(rusqlite::Error::QueryReturnedNoRows) => {
            println!("未找到对应的导航台。");
            return Ok(None);
        }
        Err(error) => return Err(error).context("查询导航台起始记录失败"),
    };

    let mut stmt = conn.prepare(
        "SELECT Ident, Type, Name, Freq, Channel, Usage, Latitude, Longtitude, Elevation FROM navaids WHERE ID > ? ORDER BY Latitude ASC",
    )?;
    let rows = stmt.query_map(params![start_id], |row| {
        Ok(NavaidRow {
            ident: row.get(0)?,
            type_code: row_opt_i64(row, 1)?.unwrap_or_default(),
            name: row_string(row, 2)?,
            freq: row_opt_i64(row, 3)?.unwrap_or_default(),
            usage: row_string(row, 5)?,
            latitude: row_opt_f64(row, 6)?.unwrap_or_default(),
            longitude: row_opt_f64(row, 7)?.unwrap_or_default(),
        })
    })?;

    let mut body = String::new();
    for row in rows {
        append_navaid_row(&mut body, &row?);
    }
    Ok((!body.is_empty()).then_some(body))
}

fn build_wpnavfix_data(conn: &Connection) -> Result<Option<String>> {
    let second_id: i64 = match conn.query_row(
        "SELECT ID FROM waypoints WHERE Ident = '89E80' ORDER BY ID LIMIT 1 OFFSET 1",
        [],
        |row| row.get(0),
    ) {
        Ok(id) => id,
        Err(rusqlite::Error::QueryReturnedNoRows) => {
            println!("没有足够的记录");
            return Ok(None);
        }
        Err(error) => return Err(error).context("查询 wpnavfix 起始记录失败"),
    };

    let mut waypoint_stmt = conn.prepare(
        "SELECT Ident, Latitude, Longtitude FROM waypoints WHERE ID > ? ORDER BY Latitude ASC",
    )?;
    let rows = waypoint_stmt.query_map(params![second_id], |row| {
        let mut ident: String = row.get(0)?;
        if ident == "AIDW5" {
            ident = "CH050".to_string();
        }
        let latitude = row_opt_f64(row, 1)?.unwrap_or_default();
        let longitude = row_opt_f64(row, 2)?.unwrap_or_default();
        Ok((ident, latitude, longitude))
    })?;

    let mut body = String::new();
    for row in rows {
        let (ident, latitude, longitude) = row?;
        let _ = writeln!(
            body,
            "{ident:<24}{ident:<5}{latitude:>10.6}{longitude:>11.6}"
        );
    }
    Ok((!body.is_empty()).then_some(body))
}

fn write_optional_supplemental_file(
    navdata_path: &Path,
    file_name: &str,
    contents: Option<&str>,
) -> Result<()> {
    let output_folder = navdata_path.join("Supplemental");
    let output_file = output_folder.join(file_name);
    let Some(contents) = contents.filter(|contents| !contents.trim().is_empty()) else {
        if output_file.exists()
            && fs::read_to_string(&output_file).is_ok_and(|existing| existing.trim().is_empty())
        {
            fs::remove_file(&output_file)
                .with_context(|| format!("无法删除空的增量文件 {}", output_file.display()))?;
        }
        return Ok(());
    };
    fs::create_dir_all(&output_folder)?;
    crate::common::write_text_file(&output_file, contents)?;
    Ok(())
}

fn append_navaid_row(buffer: &mut String, row: &NavaidRow) {
    let type_text = match row.type_code {
        1 => "VOR",
        2 | 4 => "VORD",
        3 => "VOR",
        9 => "DME",
        5 => "NDB",
        7 => "NDBD",
        8 => "ILSD",
        _ => "",
    };
    let frequency = format_ils_frequency(row.freq);
    let final_letter = match row.type_code {
        5 | 7 => 'N',
        8 => 'T',
        _ => match row.usage.chars().last().unwrap_or_default() {
            'B' => 'H',
            'H' => 'U',
            'T' => 'L',
            other => other,
        },
    };
    let _ = writeln!(
        buffer,
        "{:<24}{:<5}{:<4}{:>10.6}{:>11.6}{}{final_letter}",
        fixed_width_text(&row.name, 24),
        row.ident,
        type_text,
        row.latitude,
        row.longitude,
        frequency,
    );
}

fn fixed_width_text(text: &str, len: usize) -> String {
    let truncated = text.chars().take(len).collect::<String>();
    format!("{truncated:<len$}")
}

#[cfg(test)]
mod tests {
    use super::{
        append_navaid_row, append_wpnavapt_row, fixed_width_text, format_ils_frequency,
        normalized_supp_value, NavaidRow, RunwayTask,
    };

    #[test]
    fn decodes_packed_bcd_frequency() {
        assert_eq!(format_ils_frequency(18_055_168), "113.80");
        assert_eq!(format_ils_frequency(53_608_448), "332.00");
    }

    #[test]
    fn serializes_current_ifly_fixed_width_records() {
        let mut navaid = String::new();
        append_navaid_row(
            &mut navaid,
            &NavaidRow {
                ident: "ZFX".to_string(),
                type_code: 3,
                name: "PHOENIX MCMURDO STATION EXTRA".to_string(),
                freq: 18_055_168,
                usage: "H".to_string(),
                latitude: -77.947_422,
                longitude: 166.734_272,
            },
        );
        let navaid = navaid.trim_end_matches('\n');
        assert_eq!(navaid.len(), 61);
        assert!(navaid.starts_with("PHOENIX MCMURDO STATION"));
        assert!(navaid.contains("ZFX  VOR "));
        assert!(navaid.ends_with("113.80U"));

        let mut runway = String::new();
        append_wpnavapt_row(
            &mut runway,
            &RunwayTask {
                airport_name: "JAMES ARMSTRONG RICHARDSON INTERNATIONAL".to_string(),
                icao: "CYWG".to_string(),
                ident: "36".to_string(),
                length: 11_000.0,
                latitude: 49.91,
                longitude: -97.24,
                true_heading: 359.0,
                frequency: "000.00".to_string(),
                elevation: 784.0,
            },
            0.0,
        );
        let runway = runway.trim_end_matches('\n');
        assert_eq!(runway.len(), 74);
        assert!(runway.starts_with("JAMES ARMSTRONG RICHARD"));
    }

    #[test]
    fn applies_ifly_supp_defaults() {
        assert_eq!(normalized_supp_value(None, "250"), "250");
        assert_eq!(
            normalized_supp_value(Some("0".to_string()), "18000"),
            "18000"
        );
        assert_eq!(
            normalized_supp_value(Some("11800".to_string()), "18000"),
            "11800"
        );
        assert_eq!(
            fixed_width_text("ABCDEFGHIJKLMNOPQRSTUVWXYZ", 24),
            "ABCDEFGHIJKLMNOPQRSTUVWX"
        );
    }
}
