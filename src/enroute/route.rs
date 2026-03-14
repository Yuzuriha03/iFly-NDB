use std::collections::{BTreeMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Result};
use encoding_rs::GBK;
use geographiclib_rs::{Geodesic, InverseGeodesic};
use regex::Regex;
use rusqlite::{params, Connection};

#[derive(Clone, Debug)]
struct RouteLine {
    segment_number: usize,
    fix_ident: String,
    latitude: String,
    longitude: String,
}

#[derive(Clone, Debug)]
struct CsvSegment {
    route_ident: String,
    start_ident: String,
    start_type: String,
    start_lat_dms: String,
    start_lon_dms: String,
    end_ident: String,
    end_type: String,
    end_lat_dms: String,
    end_lon_dms: String,
}

pub fn wpnavrte(conn: &Connection, csv_file_path: &Path, navdata_path: &Path) -> Result<PathBuf> {
    let csv_segments = load_csv_segments(csv_file_path)?;
    if csv_segments.is_empty() {
        return Err(anyhow!("CSV 中没有可转换的航路段"));
    }

    let mut airway_segments = Vec::new();
    let mut segment_number = 1usize;
    let mut previous_airway_ident = String::new();

    for (index, segment) in csv_segments.iter().enumerate() {
        let airway_ident = segment.route_ident.clone();
        let mut start_ident = normalize_route_fix_ident(&segment.start_ident);
        let start_lat = dms_to_decimal_latitude(&segment.start_lat_dms)?;
        let start_lon = dms_to_decimal_longitude(&segment.start_lon_dms)?;
        let (updated_start_lat, updated_start_lon) = update_coordinates(
            conn,
            &start_ident,
            start_lat,
            start_lon,
            &segment.start_type,
        )?;

        if airway_ident != previous_airway_ident && !previous_airway_ident.is_empty() {
            let previous_segment = &csv_segments[index - 1];
            let end_ident = normalize_route_fix_ident(&previous_segment.end_ident);
            let end_lat = dms_to_decimal_latitude(&previous_segment.end_lat_dms)?;
            let end_lon = dms_to_decimal_longitude(&previous_segment.end_lon_dms)?;
            let (updated_end_lat, updated_end_lon) = update_coordinates(
                conn,
                &end_ident,
                end_lat,
                end_lon,
                &previous_segment.end_type,
            )?;
            airway_segments.push(format!(
                "{} {:03} {} {:.6} {:.6}",
                previous_airway_ident,
                segment_number,
                end_ident,
                updated_end_lat,
                updated_end_lon,
            ));
            segment_number = 1;
        }

        airway_segments.push(format!(
            "{} {:03} {} {:.6} {:.6}",
            airway_ident,
            segment_number,
            start_ident,
            updated_start_lat,
            updated_start_lon,
        ));
        segment_number += 1;
        previous_airway_ident = airway_ident;
        start_ident.clear();
    }

    let last = csv_segments.last().unwrap();
    let end_ident = normalize_route_fix_ident(&last.end_ident);
    let end_lat = dms_to_decimal_latitude(&last.end_lat_dms)?;
    let end_lon = dms_to_decimal_longitude(&last.end_lon_dms)?;
    let (updated_end_lat, updated_end_lon) = update_coordinates(conn, &end_ident, end_lat, end_lon, &last.end_type)?;
    airway_segments.push(format!(
        "{} {:03} {} {:.6} {:.6}",
        previous_airway_ident,
        segment_number,
        end_ident,
        updated_end_lat,
        updated_end_lon,
    ));

    airway_segments.sort_by(|left, right| left.split_whitespace().next().cmp(&right.split_whitespace().next()));

    let output_folder = navdata_path.join("Supplemental");
    fs::create_dir_all(&output_folder)?;
    let output_file = output_folder.join("wpnavrte.txt");
    fs::write(&output_file, airway_segments.join("\n") + "\n")?;
    Ok(output_file)
}

pub fn check_route(file1: &Path, file2: &Path) -> Result<()> {
    let file1_dict = read_file_to_dict(file1)?;
    let file2_dict = read_file_to_dict(file2)?;
    let processed = process_dicts(&file1_dict, file2_dict);
    save_checked_routes(file2, &processed)?;
    Ok(())
}

pub fn insert_route(file1: &Path, file2: &Path) -> Result<()> {
    let mut file1_dict = read_file_to_dict(file1)?;
    let file2_dict = read_file_to_dict(file2)?;
    compare_and_insert(&mut file1_dict, &file2_dict);
    save_route_dict(file1, &file1_dict)?;
    save_sample_to_file(file2)?;
    Ok(())
}

pub fn order_route(file1: &Path) -> Result<()> {
    let mut dict = read_file_to_dict(file1)?;
    for segments in dict.values_mut() {
        for (index, segment) in segments.iter_mut().enumerate() {
            segment.segment_number = index + 1;
        }
    }
    save_route_dict(file1, &dict)
}

fn load_csv_segments(csv_file_path: &Path) -> Result<Vec<CsvSegment>> {
    let bytes = fs::read(csv_file_path)
        .with_context(|| format!("无法读取 CSV 文件: {}", csv_file_path.display()))?;
    let (decoded, _, _) = GBK.decode(&bytes);

    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_reader(decoded.as_bytes());
    let headers = reader.headers()?.clone();

    let idx = |name: &str| -> Result<usize> {
        headers
            .iter()
            .position(|header| header == name)
            .ok_or_else(|| anyhow!("CSV 缺少列: {name}"))
    };

    let route_idx = idx("TXT_DESIG")?;
    let start_idx = idx("CODE_POINT_START")?;
    let start_type_idx = idx("CODE_TYPE_START")?;
    let start_lat_idx = idx("GEO_LAT_START_ACCURACY")?;
    let start_lon_idx = idx("GEO_LONG_START_ACCURACY")?;
    let end_idx = idx("CODE_POINT_END")?;
    let end_type_idx = idx("CODE_TYPE_END")?;
    let end_lat_idx = idx("GEO_LAT_END_ACCURACY")?;
    let end_lon_idx = idx("GEO_LONG_END_ACCURACY")?;

    let mut segments = Vec::new();
    for record in reader.records() {
        let record = record?;
        segments.push(CsvSegment {
            route_ident: record.get(route_idx).unwrap_or_default().trim().to_string(),
            start_ident: record.get(start_idx).unwrap_or_default().trim().to_string(),
            start_type: record.get(start_type_idx).unwrap_or_default().trim().to_string(),
            start_lat_dms: record.get(start_lat_idx).unwrap_or_default().trim().to_string(),
            start_lon_dms: record.get(start_lon_idx).unwrap_or_default().trim().to_string(),
            end_ident: record.get(end_idx).unwrap_or_default().trim().to_string(),
            end_type: record.get(end_type_idx).unwrap_or_default().trim().to_string(),
            end_lat_dms: record.get(end_lat_idx).unwrap_or_default().trim().to_string(),
            end_lon_dms: record.get(end_lon_idx).unwrap_or_default().trim().to_string(),
        });
    }
    Ok(segments)
}

fn dms_to_decimal_latitude(dms: &str) -> Result<f64> {
    if dms.len() < 7 {
        return Err(anyhow!("非法纬度 DMS: {dms}"));
    }
    let direction = &dms[0..1];
    let degrees: f64 = dms[1..3].parse()?;
    let minutes: f64 = dms[3..5].parse()?;
    let seconds: f64 = dms[5..].parse()?;
    let mut decimal = degrees + minutes / 60.0 + seconds / 3600.0;
    if direction == "S" {
        decimal = -decimal;
    }
    Ok((decimal * 100_000_000.0).round() / 100_000_000.0)
}

fn dms_to_decimal_longitude(dms: &str) -> Result<f64> {
    if dms.len() < 8 {
        return Err(anyhow!("非法经度 DMS: {dms}"));
    }
    let direction = &dms[0..1];
    let degrees: f64 = dms[1..4].parse()?;
    let minutes: f64 = dms[4..6].parse()?;
    let seconds: f64 = dms[6..].parse()?;
    let mut decimal = degrees + minutes / 60.0 + seconds / 3600.0;
    if direction == "W" {
        decimal = -decimal;
    }
    Ok((decimal * 100_000_000.0).round() / 100_000_000.0)
}

fn normalize_route_fix_ident(ident: &str) -> String {
    match ident {
        "****" => "72PCA".to_string(),
        "AIWD50/CH" => "CH050".to_string(),
        other => other.to_string(),
    }
}

fn update_coordinates(conn: &Connection, ident: &str, latitude: f64, longitude: f64, point_type: &str) -> Result<(f64, f64)> {
    let table_name = match point_type {
        "VORDME" | "NDB" => Some("Navaids"),
        "DESIGNATED_POINT" => Some("Waypoints"),
        _ => None,
    };

    let Some(table_name) = table_name else {
        return Ok((latitude, longitude));
    };

    let mut stmt = conn.prepare(&format!("SELECT Latitude, Longtitude FROM {table_name} WHERE Ident = ?"))?;
    let coords = stmt.query_map(params![ident], |row| Ok((row.get::<_, f64>(0)?, row.get::<_, f64>(1)?)))?;

    let geodesic = Geodesic::wgs84();
    let mut best_match = None;
    let mut min_distance_nm = f64::MAX;
    for coord in coords {
        let (lat, lon) = coord?;
        let distance_m: f64 = geodesic.inverse(latitude, longitude, lat, lon);
        let distance_nm = distance_m / 1852.0;
        if distance_nm <= 5.0 && distance_nm < min_distance_nm {
            best_match = Some((lat, lon));
            min_distance_nm = distance_nm;
        }
    }

    Ok(best_match.unwrap_or((latitude, longitude)))
}

fn read_file_to_dict(file_path: &Path) -> Result<BTreeMap<String, Vec<RouteLine>>> {
    let contents = fs::read_to_string(file_path)
        .with_context(|| format!("无法读取航路文件: {}", file_path.display()))?;
    let mut dict = BTreeMap::new();
    for line in contents.lines() {
        if line.starts_with(';') || line.trim().is_empty() {
            continue;
        }
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() < 5 {
            continue;
        }
        dict.entry(parts[0].to_string()).or_insert_with(Vec::new).push(RouteLine {
            segment_number: parts[1].parse().unwrap_or_default(),
            fix_ident: parts[2].to_string(),
            latitude: parts[3].to_string(),
            longitude: parts[4].to_string(),
        });
    }
    Ok(dict)
}

fn process_dicts(
    dict1: &BTreeMap<String, Vec<RouteLine>>,
    mut dict2: BTreeMap<String, Vec<RouteLine>>,
) -> BTreeMap<String, Vec<RouteLine>> {
    for (route_ident, left_segments) in dict1 {
        if let Some(right_segments) = dict2.get_mut(route_ident) {
            let left_first = left_segments.iter().min_by_key(|segment| segment.segment_number).map(|segment| segment.fix_ident.clone());
            let right_first = right_segments.iter().min_by_key(|segment| segment.segment_number).map(|segment| segment.fix_ident.clone());
            if left_first != right_first {
                let reversed_fixes: Vec<(String, String, String)> = right_segments
                    .iter()
                    .cloned()
                    .collect::<Vec<_>>()
                    .into_iter()
                    .rev()
                    .map(|segment| (segment.fix_ident, segment.latitude, segment.longitude))
                    .collect();
                for (index, segment) in right_segments.iter_mut().enumerate() {
                    segment.fix_ident = reversed_fixes[index].0.clone();
                    segment.latitude = reversed_fixes[index].1.clone();
                    segment.longitude = reversed_fixes[index].2.clone();
                }
            }
        }
    }
    dict2
}

fn save_checked_routes(original_file: &Path, processed_dict: &BTreeMap<String, Vec<RouteLine>>) -> Result<()> {
    let original_lines = fs::read_to_string(original_file)?;
    let pattern = Regex::new(r"P\d{2,3}")?;
    let mut naip_route_idents = HashSet::new();

    for line in original_lines.lines() {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() >= 3 && pattern.is_match(parts[2]) {
            naip_route_idents.insert(parts[0].to_string());
        }
    }

    let mut output = Vec::new();
    for line in original_lines.lines() {
        if line.starts_with(';') || line.trim().is_empty() {
            output.push(line.to_string());
            continue;
        }
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() < 5 {
            output.push(line.to_string());
            continue;
        }

        let route_ident = parts[0];
        if line.starts_with("XX") {
            continue;
        }
        if matches!(route_ident.chars().next(), Some('A' | 'B' | 'G' | 'L' | 'M' | 'R' | 'V' | 'W'))
            && !naip_route_idents.contains(route_ident)
        {
            continue;
        }

        let segment_number: usize = parts[1].parse().unwrap_or_default();
        if let Some(segment) = processed_dict
            .get(route_ident)
            .and_then(|segments| segments.iter().find(|segment| segment.segment_number == segment_number))
        {
            output.push(format!(
                "{} {:03} {} {} {}",
                route_ident,
                segment_number,
                segment.fix_ident,
                segment.latitude,
                segment.longitude,
            ));
        } else {
            output.push(line.to_string());
        }
    }

    fs::write(original_file, output.join("\n") + "\n")?;
    Ok(())
}

fn compare_and_insert(file1_dict: &mut BTreeMap<String, Vec<RouteLine>>, file2_dict: &BTreeMap<String, Vec<RouteLine>>) {
    for (route_ident, file2_segments) in file2_dict {
        if file2_segments.is_empty() {
            continue;
        }

        if let Some(file1_segments) = file1_dict.get_mut(route_ident) {
            let first_match = file1_segments
                .iter()
                .position(|segment| segment.fix_ident == file2_segments[0].fix_ident);
            let last_match = file1_segments
                .iter()
                .rposition(|segment| segment.fix_ident == file2_segments[file2_segments.len() - 1].fix_ident);

            if let (Some(start_index), Some(end_index)) = (first_match, last_match) {
                if start_index <= end_index {
                    file1_segments.splice(start_index..=end_index, file2_segments.clone());
                } else {
                    file1_segments.splice(start_index..start_index, file2_segments.clone());
                }
            } else {
                file1_segments.extend(file2_segments.clone());
            }
        } else {
            file1_dict.insert(route_ident.clone(), file2_segments.clone());
        }
    }
}

fn save_route_dict(file_path: &Path, dict: &BTreeMap<String, Vec<RouteLine>>) -> Result<()> {
    let mut lines = Vec::new();
    for (route_ident, segments) in dict {
        for segment in segments {
            lines.push(format!(
                "{} {:03} {} {} {}",
                route_ident,
                segment.segment_number,
                segment.fix_ident,
                segment.latitude,
                segment.longitude,
            ));
        }
    }
    fs::write(file_path, lines.join("\n") + "\n")?;
    Ok(())
}

fn save_sample_to_file(file_path: &Path) -> Result<()> {
    let sample_text = ";Supplemental Navaid Database (Option)\n;;\n;Data format is same as P3D_root\\iFly\\737MAX\\navdata\\\n;;\n;If any route in this file have same identifier as in\n;Main Navaid Database, FMC will delete route data in\n;the Main Navaid Database\n;;\n;This is a sample file\n;-------------------------------------------------------------\nTEST 001 TEST1 33.114350 139.788483\nTEST 002 TEST2 33.193211 138.972397\nTEST 003 TEST3 33.447742 135.794495\n";
    fs::write(file_path, sample_text)?;
    Ok(())
}