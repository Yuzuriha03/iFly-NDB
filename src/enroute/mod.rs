pub mod route;

use std::collections::HashMap;
use std::fmt::Write as _;
use std::fs;
use std::path::Path;
use std::sync::OnceLock;

use anyhow::{Context, Result};
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

pub fn run(
    conn: &mut Connection,
    route_file: &Path,
    navdata_path: &Path,
    csv_path: &Path,
) -> Result<()> {
    let Some(start_airport_id) = start_airport_id(conn)? else {
        return Ok(());
    };
    airport(conn, start_airport_id, navdata_path)?;

    supp(conn, start_airport_id, navdata_path)?;

    wpnavapt(conn, start_airport_id, navdata_path)?;

    wpnavaid(conn, navdata_path)?;

    wpnavfix(conn, navdata_path)?;

    let generated_route_file = route::wpnavrte(conn, csv_path, navdata_path)?;

    let checked_routes = route::check_route(route_file, &generated_route_file)?;

    route::insert_and_order_route(route_file, &generated_route_file, checked_routes)?;
    println!("Enroute数据转换完毕");
    Ok(())
}

fn start_airport_id(conn: &Connection) -> Result<Option<i64>> {
    let start_id = conn.query_row(
        "SELECT ID FROM airports WHERE ICAO = 'ZYYJ'",
        [],
        |row| row.get::<_, i64>(0),
    );

    match start_id {
        Ok(id) => Ok(Some(id + 1)),
        Err(rusqlite::Error::QueryReturnedNoRows) => {
            println!("未找到对应的机场。");
            Ok(None)
        }
        Err(error) => Err(error).context("查询起始机场失败"),
    }
}

fn airport(conn: &Connection, start_id: i64, navdata_path: &Path) -> Result<()> {
    let mut stmt = conn.prepare(
        "SELECT ICAO, Latitude, Longtitude FROM airports WHERE ID >= ? ORDER BY Latitude ASC",
    )?;
    let rows = stmt.query_map(params![start_id], |row| {
        let icao: String = row.get(0)?;
        let latitude: f64 = row.get(1)?;
        let longitude: f64 = row.get(2)?;
        Ok((latitude, format!("{icao}{latitude:>10.6}{longitude:>11.6}")))
    })?;

    let output_folder = navdata_path.join("Supplemental");
    fs::create_dir_all(&output_folder)?;
    let output_file = output_folder.join("airports.dat");

    let mut lines = Vec::new();
    for row in rows {
        lines.push(row?.1);
    }
    fs::write(&output_file, lines.join("\n") + "\n")?;
    println!("airport.dat已保存到{}", output_file.display());
    Ok(())
}

fn supp(conn: &Connection, start_airport_id: i64, navdata_path: &Path) -> Result<()> {
    let output_folder = navdata_path.join("Supplemental").join("Supp");
    fs::create_dir_all(&output_folder)?;

    let mut stmt = conn.prepare(
        "SELECT ICAO, TransitionAltitude, TransitionLevel, SpeedLimit FROM airports WHERE ID >= ?",
    )?;
    let rows = stmt.query_map(params![start_airport_id], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row_opt_string(row, 1)?.unwrap_or_else(|| "None".to_string()),
            row_opt_string(row, 2)?.unwrap_or_else(|| "None".to_string()),
            row_opt_string(row, 3)?.unwrap_or_else(|| "None".to_string()),
        ))
    })?;

    for row in rows {
        let (icao, transition_altitude, transition_level, speed_limit) = row?;
        let file_contents = [
            "[Speed_Transition]".to_string(),
            format!("Speed={speed_limit}"),
            "Altitude=10000".to_string(),
            "[Transition_Altitude]".to_string(),
            format!("Altitude={transition_altitude}"),
            "[Transition_Level]".to_string(),
            format!("Altitude={transition_level}"),
        ];
        fs::write(output_folder.join(format!("{icao}.supp")), file_contents.join("\n"))?;
    }

    println!("supp文件已保存到 {}", output_folder.display());
    Ok(())
}

fn wpnavapt(conn: &Connection, start_airport_id: i64, navdata_path: &Path) -> Result<()> {
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
        let (airport_name, icao, runway_id, ident, true_heading, length, latitude, longitude, elevation) = runway_row?;
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
        return Ok(());
    }

    tasks.sort_by(|left, right| left.icao.cmp(&right.icao).then(left.ident.cmp(&right.ident)));

    let declinations = magnetic::batch_get_magnetic_variations(
        &tasks.iter().map(|task| (task.latitude, task.longitude)).collect::<Vec<_>>(),
    )?;

    let output_folder = navdata_path.join("Supplemental");
    fs::create_dir_all(&output_folder)?;
    let output_file = output_folder.join("wpnavapt.txt");
    let mut body = String::with_capacity(tasks.len().saturating_mul(72));
    for (task, declination) in tasks.into_iter().zip(declinations) {
        append_wpnavapt_row(&mut body, &task, declination);
    }
    fs::write(&output_file, body)?;

    println!("已保存到 {}", output_file.display());
    Ok(())
}

fn append_wpnavapt_row(buffer: &mut String, task: &RunwayTask, declination: f64) {
    let magnetic_heading = (task.true_heading - declination).round() as i64;
    let _ = writeln!(
        buffer,
        "{:<24}{}{: <3}{:05}{:03}{:>10.6}{:>11.6}{}{:03}{:05}",
        task.airport_name,
        task.icao,
        task.ident,
        task.length.round() as i64,
        magnetic_heading,
        task.latitude,
        task.longitude,
        task.frequency,
        magnetic_heading,
        task.elevation.round() as i64,
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
        let frequency = freq_value
            .map(format_ils_frequency)
            .unwrap_or_else(|| "000.00".to_string());
        cache.insert(runway_id, frequency);
    }
    Ok(cache)
}

fn format_ils_frequency(freq: i64) -> String {
    let mut frequency = format!("{:X}", freq).parse::<f64>().unwrap_or(0.0);
    while frequency >= 1000.0 {
        frequency /= 10.0;
    }
    format!("{frequency:.2}")
}

fn wpnavaid(conn: &Connection, navdata_path: &Path) -> Result<()> {
    let start_id: i64 = match conn.query_row(
        "SELECT ID FROM navaids WHERE Name = 'DEXIN YANJI'",
        [],
        |row| row.get(0),
    ) {
        Ok(start_id) => start_id,
        Err(rusqlite::Error::QueryReturnedNoRows) => {
            println!("未找到对应的导航台。");
            return Ok(());
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

    let output_folder = navdata_path.join("Supplemental");
    fs::create_dir_all(&output_folder)?;
    let output_file = output_folder.join("wpnavaid.txt");
    let mut body = String::new();
    for row in rows {
        append_navaid_row(&mut body, &row?);
    }
    fs::write(&output_file, body)?;
    println!("wpnavaid.txt已保存到 {}", output_file.display());
    Ok(())
}

fn wpnavfix(conn: &Connection, navdata_path: &Path) -> Result<()> {
    let second_id: i64 = match conn.query_row(
        "SELECT ID FROM waypoints WHERE Ident = '89E80' ORDER BY ID LIMIT 1 OFFSET 1",
        [],
        |row| row.get(0),
    ) {
        Ok(id) => id,
        Err(rusqlite::Error::QueryReturnedNoRows) => {
            println!("没有足够的记录");
            return Ok(());
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

    let output_folder = navdata_path.join("Supplemental");
    fs::create_dir_all(&output_folder)?;
    let output_file = output_folder.join("wpnavfix.txt");
    let mut body = String::new();
    for row in rows {
        let (ident, latitude, longitude) = row?;
        let _ = writeln!(body, "{:<24}{:<5}{:>10.6}{:>11.6}", ident, ident, latitude, longitude);
    }
    fs::write(&output_file, body)?;
    println!("wpnavfix已保存到 {}", output_file.display());
    Ok(())
}

fn append_navaid_row(buffer: &mut String, row: &NavaidRow) {
    let type_text = match row.type_code {
        1 => "VOR",
        2 | 4 => "VORD",
        3 | 9 => "DME",
        5 => "NDB",
        7 => "NDBD",
        8 => "ILSD",
        _ => "",
    };
    let frequency = format_ils_frequency(row.freq);
    let final_letter = row.usage.chars().last().unwrap_or_default();
    let _ = writeln!(
        buffer,
        "{:<24}{:<5}{:<4}{:>10.6}{:>11.6}{}{final_letter}",
        truncate_left(&row.name, 24),
        row.ident,
        type_text,
        row.latitude,
        row.longitude,
        frequency,
    );
}

fn truncate_left(text: &str, len: usize) -> String {
    let padded = format!("{text:<len$}");
    if padded.len() <= len {
        padded
    } else {
        padded[padded.len() - len..].to_string()
    }
}

static EMPTY: OnceLock<String> = OnceLock::new();

#[allow(dead_code)]
fn empty_string() -> &'static str {
    EMPTY.get_or_init(String::new)
}
