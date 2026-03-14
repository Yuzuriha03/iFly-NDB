pub mod route;

use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::OnceLock;

use anyhow::{Context, Result};
use rayon::prelude::*;
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

pub fn run(conn: &mut Connection, route_file: &Path, navdata_path: &Path, csv_path: &Path) -> Result<()> {
    let Some(start_airport_id) = start_airport_id(conn)? else {
        return Ok(());
    };
    let started = std::time::Instant::now();

    airport(conn, start_airport_id, navdata_path)?;
    supp(conn, start_airport_id, navdata_path)?;
    wpnavapt(conn, start_airport_id, navdata_path)?;
    wpnavaid(conn, navdata_path)?;
    wpnavfix(conn, navdata_path)?;
    let generated_route_file = route::wpnavrte(conn, csv_path, navdata_path)?;
    route::check_route(route_file, &generated_route_file)?;
    route::insert_route(route_file, &generated_route_file)?;
    route::order_route(route_file)?;

    println!("Enroute数据转换完毕，用时：{:.3}秒", started.elapsed().as_secs_f64());
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
    let start_runway_exists = conn.query_row(
        "SELECT ID FROM runways WHERE AirportID = ? LIMIT 1",
        params![start_airport_id],
        |row| row.get::<_, i64>(0),
    );
    if start_runway_exists.is_err() {
        println!("未找到对应的RunwayID");
        return Ok(());
    }

    let mut airport_stmt = conn.prepare("SELECT ID, Name, ICAO FROM airports WHERE ID >= ?")?;
    let airport_rows: Vec<(i64, String, String)> = airport_stmt
        .query_map(params![start_airport_id], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })?
        .collect::<rusqlite::Result<_>>()?;

    if airport_rows.is_empty() {
        println!("未找到需要处理的跑道数据");
        return Ok(());
    }

    let ils_frequency_cache = load_ils_frequency_cache(conn)?;
    let mut tasks = Vec::new();

    let mut runway_stmt = conn.prepare(
        "SELECT ID, Ident, TrueHeading, Length, Latitude, Longtitude, Elevation FROM runways WHERE AirportID = ?",
    )?;

    for (airport_id, airport_name, icao) in airport_rows {
        let runway_rows = runway_stmt.query_map(params![airport_id], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row_opt_f64(row, 2)?.unwrap_or_default(),
                row_opt_f64(row, 3)?.unwrap_or_default(),
                row_opt_f64(row, 4)?.unwrap_or_default(),
                row_opt_f64(row, 5)?.unwrap_or_default(),
                row_opt_f64(row, 6)?.unwrap_or_default(),
            ))
        })?;

        for runway_row in runway_rows {
            let (runway_id, ident, true_heading, length, latitude, longitude, elevation) = runway_row?;
            let frequency = ils_frequency_cache
                .get(&runway_id)
                .cloned()
                .unwrap_or_else(|| "000.00".to_string());
            tasks.push(RunwayTask {
                airport_name: airport_name.clone(),
                icao: icao.clone(),
                ident,
                length,
                latitude,
                longitude,
                true_heading,
                frequency,
                elevation,
            });
        }
    }

    if tasks.is_empty() {
        println!("未找到需要处理的跑道数据");
        return Ok(());
    }

    let declinations = magnetic::batch_get_magnetic_variations(
        &tasks.iter().map(|task| (task.latitude, task.longitude)).collect::<Vec<_>>(),
    )?;

    let mut rows: Vec<(String, String)> = tasks
        .into_par_iter()
        .zip(declinations.into_par_iter())
        .map(|(task, declination)| {
            let magnetic_heading = (task.true_heading - declination).round() as i64;
            let line = format!(
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
            (format!("{}{}", task.icao, task.ident), line)
        })
        .collect();

    rows.sort_by(|left, right| left.0.cmp(&right.0));

    let output_folder = navdata_path.join("Supplemental");
    fs::create_dir_all(&output_folder)?;
    let output_file = output_folder.join("wpnavapt.txt");
    let body = rows.into_iter().map(|(_, line)| line).collect::<Vec<_>>().join("\n") + "\n";
    fs::write(&output_file, body)?;

    println!("已保存到 {}", output_file.display());
    Ok(())
}

fn load_ils_frequency_cache(conn: &Connection) -> Result<HashMap<i64, String>> {
    let mut terminal_stmt = conn.prepare("SELECT RwyID, ilsID FROM terminals WHERE RwyID IS NOT NULL AND ilsID IS NOT NULL")?;
    let runway_ils_pairs: Vec<(i64, i64)> = terminal_stmt
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
        .collect::<rusqlite::Result<_>>()?;

    let mut ils_stmt = conn.prepare("SELECT Freq FROM ILSes WHERE ID = ?")?;
    let mut cache = HashMap::new();
    for (runway_id, ils_id) in runway_ils_pairs {
        if cache.contains_key(&runway_id) {
            continue;
        }
        let freq_value = ils_stmt.query_row(params![ils_id], |row| row_opt_i64(row, 0))?;
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
        "SELECT Ident, Type, Name, Freq, Channel, Usage, Latitude, Longtitude, Elevation FROM navaids WHERE ID > ?",
    )?;
    let rows = stmt.query_map(params![start_id], |row| {
        let ident: String = row.get(0)?;
        let type_code = row_opt_i64(row, 1)?.unwrap_or_default();
        let name = row_string(row, 2)?;
        let freq = row_opt_i64(row, 3)?.unwrap_or_default();
        let usage = row_string(row, 5)?;
        let latitude = row_opt_f64(row, 6)?.unwrap_or_default();
        let longitude = row_opt_f64(row, 7)?.unwrap_or_default();
        Ok((latitude, format_navaid_row(&ident, type_code, &name, freq, &usage, latitude, longitude)))
    })?;

    let mut converted = rows.collect::<rusqlite::Result<Vec<_>>>()?;
    converted.sort_by(|left, right| left.0.partial_cmp(&right.0).unwrap_or(std::cmp::Ordering::Equal));

    let output_folder = navdata_path.join("Supplemental");
    fs::create_dir_all(&output_folder)?;
    let output_file = output_folder.join("wpnavaid.txt");
    let body = converted
        .into_iter()
        .map(|(_, line)| line)
        .collect::<Vec<_>>()
        .join("\n")
        + "\n";
    fs::write(&output_file, body)?;
    println!("wpnavaid.txt已保存到 {}", output_file.display());
    Ok(())
}

fn format_navaid_row(ident: &str, type_code: i64, name: &str, freq: i64, usage: &str, latitude: f64, longitude: f64) -> String {
    let type_text = match type_code {
        1 => "VOR",
        2 | 4 => "VORD",
        3 | 9 => "DME",
        5 => "NDB",
        7 => "NDBD",
        8 => "ILSD",
        _ => "",
    };
    let frequency = format_ils_frequency(freq);
    let final_letter = usage.chars().last().unwrap_or_default();
    format!(
        "{:<24}{:<5}{:<4}{:>10.6}{:>11.6}{}{final_letter}",
        truncate_left(name, 24),
        ident,
        type_text,
        latitude,
        longitude,
        frequency,
    )
}

fn wpnavfix(conn: &Connection, navdata_path: &Path) -> Result<()> {
    let mut stmt = conn.prepare("SELECT ID FROM waypoints WHERE Ident = '89E80'")?;
    let ids: Vec<i64> = stmt
        .query_map([], |row| row.get(0))?
        .collect::<rusqlite::Result<_>>()?;
    if ids.len() < 2 {
        println!("没有足够的记录");
        return Ok(());
    }
    let second_id = ids[1];

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
        Ok((
            latitude,
            format!("{:<24}{:<5}{:>10.6}{:>11.6}", ident, ident, latitude, longitude),
        ))
    })?;

    let converted = rows.collect::<rusqlite::Result<Vec<_>>>()?;
    let output_folder = navdata_path.join("Supplemental");
    fs::create_dir_all(&output_folder)?;
    let output_file = output_folder.join("wpnavfix.txt");
    let body = converted
        .into_iter()
        .map(|(_, line)| line)
        .collect::<Vec<_>>()
        .join("\n")
        + "\n";
    fs::write(&output_file, body)?;
    println!("wpnavfix已保存到 {}", output_file.display());
    Ok(())
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
