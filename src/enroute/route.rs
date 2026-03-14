use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Result};
use encoding_rs::GBK;
use geographiclib_rs::{Geodesic, InverseGeodesic};
use regex::Regex;
use rusqlite::{params_from_iter, Connection};

#[derive(Clone, Debug)]
pub(crate) struct RouteLine {
    segment_number: usize,
    fix_ident: String,
    latitude: String,
    longitude: String,
}

pub(crate) struct CheckedRouteData {
    existing_routes: BTreeMap<String, Vec<RouteLine>>,
    checked_routes: BTreeMap<String, Vec<RouteLine>>,
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

struct GeneratedAirwaySegment {
    route_ident: String,
    segment_number: usize,
    fix_ident: String,
    latitude: f64,
    longitude: f64,
}

type CoordinateKey = (u64, u64);
type ResolvedCoordinateCache = HashMap<String, HashMap<CoordinateKey, (f64, f64)>>;
type CoordinateCandidates = HashMap<String, Vec<(f64, f64)>>;
const IDENT_QUERY_CHUNK_SIZE: usize = 900;

struct CoordinateResolver {
    geodesic: Geodesic,
    navaid_candidates: HashMap<String, Vec<(f64, f64)>>,
    waypoint_candidates: HashMap<String, Vec<(f64, f64)>>,
    navaid_resolved_cache: ResolvedCoordinateCache,
    waypoint_resolved_cache: ResolvedCoordinateCache,
}

pub fn wpnavrte(
    conn: &mut Connection,
    csv_file_path: &Path,
    navdata_path: &Path,
) -> Result<PathBuf> {
    let csv_segments = load_csv_segments(csv_file_path)?;
    if csv_segments.is_empty() {
        return Err(anyhow!("CSV 中没有可转换的航路段"));
    }

    let mut coordinate_resolver = CoordinateResolver::new(conn, &csv_segments)?;

    let mut airway_segments = Vec::with_capacity(csv_segments.len().saturating_add(1));
    let mut segment_number = 1usize;
    let mut previous_airway_ident = String::new();

    for (index, segment) in csv_segments.iter().enumerate() {
        let airway_ident = segment.route_ident.clone();
        let start_ident = segment.start_ident.clone();
        let start_lat = dms_to_decimal_latitude(&segment.start_lat_dms)?;
        let start_lon = dms_to_decimal_longitude(&segment.start_lon_dms)?;
        let (updated_start_lat, updated_start_lon) =
            coordinate_resolver.resolve(&start_ident, start_lat, start_lon, &segment.start_type);

        if airway_ident != previous_airway_ident && !previous_airway_ident.is_empty() {
            let previous_segment = &csv_segments[index - 1];
            let end_ident = previous_segment.end_ident.clone();
            let end_lat = dms_to_decimal_latitude(&previous_segment.end_lat_dms)?;
            let end_lon = dms_to_decimal_longitude(&previous_segment.end_lon_dms)?;
            let (updated_end_lat, updated_end_lon) = coordinate_resolver.resolve(
                &end_ident,
                end_lat,
                end_lon,
                &previous_segment.end_type,
            );
            airway_segments.push(GeneratedAirwaySegment {
                route_ident: previous_airway_ident.clone(),
                segment_number,
                fix_ident: end_ident,
                latitude: updated_end_lat,
                longitude: updated_end_lon,
            });
            segment_number = 1;
        }

        airway_segments.push(GeneratedAirwaySegment {
            route_ident: airway_ident.clone(),
            segment_number,
            fix_ident: start_ident,
            latitude: updated_start_lat,
            longitude: updated_start_lon,
        });
        segment_number += 1;
        previous_airway_ident = airway_ident;
    }

    let last = csv_segments.last().unwrap();
    let end_ident = last.end_ident.clone();
    let end_lat = dms_to_decimal_latitude(&last.end_lat_dms)?;
    let end_lon = dms_to_decimal_longitude(&last.end_lon_dms)?;
    let (updated_end_lat, updated_end_lon) =
        coordinate_resolver.resolve(&end_ident, end_lat, end_lon, &last.end_type);
    airway_segments.push(GeneratedAirwaySegment {
        route_ident: previous_airway_ident,
        segment_number,
        fix_ident: end_ident,
        latitude: updated_end_lat,
        longitude: updated_end_lon,
    });

    airway_segments.sort_by(|left, right| left.route_ident.cmp(&right.route_ident));

    let output_folder = navdata_path.join("Supplemental");
    fs::create_dir_all(&output_folder)?;
    let output_file = output_folder.join("wpnavrte.txt");
    let mut output = String::with_capacity(airway_segments.len().saturating_mul(40));
    for segment in &airway_segments {
        write_generated_airway_segment(&mut output, segment);
    }
    fs::write(&output_file, output)?;
    Ok(output_file)
}

pub fn check_route(file1: &Path, file2: &Path) -> Result<CheckedRouteData> {
    let existing_routes = read_file_to_dict(file1)?;
    let generated_routes = read_generated_route_data(file2)?;
    let processed = process_dicts(&existing_routes, generated_routes.routes);
    let checked_routes = filter_checked_routes(processed, &generated_routes.naip_route_idents);
    Ok(CheckedRouteData {
        existing_routes,
        checked_routes,
    })
}

pub fn insert_and_order_route(
    file1: &Path,
    generated_file: &Path,
    checked_data: CheckedRouteData,
) -> Result<()> {
    let CheckedRouteData {
        mut existing_routes,
        checked_routes,
    } = checked_data;
    compare_and_insert(&mut existing_routes, checked_routes);
    for segments in existing_routes.values_mut() {
        for (index, segment) in segments.iter_mut().enumerate() {
            segment.segment_number = index + 1;
        }
    }
    save_route_dict(file1, &existing_routes)?;
    save_sample_to_file(generated_file)?;
    Ok(())
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
            start_ident: normalize_route_fix_ident(record.get(start_idx).unwrap_or_default().trim()),
            start_type: record.get(start_type_idx).unwrap_or_default().trim().to_string(),
            start_lat_dms: record.get(start_lat_idx).unwrap_or_default().trim().to_string(),
            start_lon_dms: record.get(start_lon_idx).unwrap_or_default().trim().to_string(),
            end_ident: normalize_route_fix_ident(record.get(end_idx).unwrap_or_default().trim()),
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

impl CoordinateResolver {
    fn new(conn: &Connection, csv_segments: &[CsvSegment]) -> Result<Self> {
        let mut navaid_idents = HashSet::new();
        let mut waypoint_idents = HashSet::new();

        for segment in csv_segments {
            collect_lookup_ident(&mut navaid_idents, &mut waypoint_idents, &segment.start_type, &segment.start_ident);
            collect_lookup_ident(&mut navaid_idents, &mut waypoint_idents, &segment.end_type, &segment.end_ident);
        }

        Ok(Self {
            geodesic: Geodesic::wgs84(),
            navaid_candidates: load_coordinate_candidates(conn, "Navaids", &navaid_idents)?,
            waypoint_candidates: load_coordinate_candidates(conn, "Waypoints", &waypoint_idents)?,
            navaid_resolved_cache: HashMap::new(),
            waypoint_resolved_cache: HashMap::new(),
        })
    }

    fn resolve(&mut self, ident: &str, latitude: f64, longitude: f64, point_type: &str) -> (f64, f64) {
        match point_type {
            "VORDME" | "NDB" => resolve_cached_coordinates(
                &self.geodesic,
                &self.navaid_candidates,
                &mut self.navaid_resolved_cache,
                ident,
                latitude,
                longitude,
            ),
            "DESIGNATED_POINT" => resolve_cached_coordinates(
                &self.geodesic,
                &self.waypoint_candidates,
                &mut self.waypoint_resolved_cache,
                ident,
                latitude,
                longitude,
            ),
            _ => (latitude, longitude),
        }
    }
}

fn resolve_cached_coordinates(
    geodesic: &Geodesic,
    candidates_by_ident: &HashMap<String, Vec<(f64, f64)>>,
    resolved_cache: &mut ResolvedCoordinateCache,
    ident: &str,
    latitude: f64,
    longitude: f64,
) -> (f64, f64) {
    let Some(candidates) = candidates_by_ident.get(ident) else {
        return (latitude, longitude);
    };

    let coordinate_key = (latitude.to_bits(), longitude.to_bits());
    if let Some(resolved) = resolved_cache
        .get(ident)
        .and_then(|entries| entries.get(&coordinate_key))
    {
        return *resolved;
    }

    if let [candidate] = candidates.as_slice() {
        let distance_m: f64 = geodesic.inverse(latitude, longitude, candidate.0, candidate.1);
        let resolved = if distance_m / 1852.0 <= 5.0 {
            *candidate
        } else {
            (latitude, longitude)
        };
        resolved_cache
            .entry(ident.to_string())
            .or_default()
            .insert(coordinate_key, resolved);
        return resolved;
    }

    let mut best_match = None;
    let mut min_distance_nm = f64::MAX;
    for (lat, lon) in candidates {
        let distance_m: f64 = geodesic.inverse(latitude, longitude, *lat, *lon);
        let distance_nm = distance_m / 1852.0;
        if distance_nm <= 5.0 && distance_nm < min_distance_nm {
            best_match = Some((*lat, *lon));
            min_distance_nm = distance_nm;
        }
    }

    let resolved = best_match.unwrap_or((latitude, longitude));
    resolved_cache
        .entry(ident.to_string())
        .or_default()
        .insert(coordinate_key, resolved);
    resolved
}

fn collect_lookup_ident(
    navaid_idents: &mut HashSet<String>,
    waypoint_idents: &mut HashSet<String>,
    point_type: &str,
    ident: &str,
) {
    match point_type {
        "VORDME" | "NDB" => {
            navaid_idents.insert(ident.to_string());
        }
        "DESIGNATED_POINT" => {
            waypoint_idents.insert(ident.to_string());
        }
        _ => {}
    }
}

fn load_coordinate_candidates(
    conn: &Connection,
    table_name: &str,
    idents: &HashSet<String>,
) -> Result<CoordinateCandidates> {
    let mut candidates = HashMap::with_capacity(idents.len());
    if idents.is_empty() {
        return Ok(candidates);
    }

    if table_name == "Waypoints" && !table_has_ident_index(conn, table_name)? {
        return load_waypoint_candidates_by_scan(conn, idents);
    }

    let ident_list = idents.iter().collect::<Vec<_>>();

    for chunk in ident_list.chunks(IDENT_QUERY_CHUNK_SIZE) {
        let placeholders = std::iter::repeat_n("?", chunk.len()).collect::<Vec<_>>().join(", ");
        let query = format!(
            "SELECT Ident, Latitude, Longtitude FROM {table_name} WHERE Ident IN ({placeholders})"
        );

        let mut stmt = conn.prepare(&query)?;
        let rows = stmt.query_map(params_from_iter(chunk.iter()), |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, f64>(1)?,
                row.get::<_, f64>(2)?,
            ))
        })?;

        for row in rows {
            let (ident, latitude, longitude) = row?;
            candidates.entry(ident).or_default().push((latitude, longitude));
        }
    }

    Ok(candidates)
}

fn table_has_ident_index(conn: &Connection, table_name: &str) -> Result<bool> {
    let pragma = format!("PRAGMA index_list('{table_name}')");
    let mut stmt = conn.prepare(&pragma)?;
    let index_names = stmt.query_map([], |row| row.get::<_, String>(1))?;

    for index_name in index_names {
        let index_name = index_name?;
        let info = format!("PRAGMA index_info('{index_name}')");
        let mut info_stmt = conn.prepare(&info)?;
        let columns = info_stmt.query_map([], |row| row.get::<_, String>(2))?;
        for column in columns {
            if column?.eq_ignore_ascii_case("Ident") {
                return Ok(true);
            }
        }
    }

    Ok(false)
}

fn load_waypoint_candidates_by_scan(
    conn: &Connection,
    idents: &HashSet<String>,
) -> Result<CoordinateCandidates> {
    let mut candidates = HashMap::with_capacity(idents.len());
    for ident in idents {
        candidates.insert(ident.clone(), Vec::new());
    }

    let query = "SELECT Ident, Latitude, Longtitude FROM Waypoints";

    let mut stmt = conn.prepare(query)?;
    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        let ident = row.get_ref(0)?.as_str()?;
        let Some(values) = candidates.get_mut(ident) else {
            continue;
        };
        values.push((row.get(1)?, row.get(2)?));
    }
    candidates.retain(|_, values| !values.is_empty());

    Ok(candidates)
}

fn read_file_to_dict(file_path: &Path) -> Result<BTreeMap<String, Vec<RouteLine>>> {
    let contents = fs::read_to_string(file_path)
        .with_context(|| format!("无法读取航路文件: {}", file_path.display()))?;
    let mut dict = BTreeMap::new();
    for line in contents.lines() {
        let Some(record) = parse_route_record(line) else {
            continue;
        };
        dict.entry(record.route_ident.to_string()).or_insert_with(Vec::new).push(RouteLine {
            segment_number: record.segment_number.parse().unwrap_or_default(),
            fix_ident: record.fix_ident.to_string(),
            latitude: record.latitude.to_string(),
            longitude: record.longitude.to_string(),
        });
    }
    Ok(dict)
}

struct GeneratedRouteData {
    routes: BTreeMap<String, Vec<RouteLine>>,
    naip_route_idents: HashSet<String>,
}

fn read_generated_route_data(file_path: &Path) -> Result<GeneratedRouteData> {
    let contents = fs::read_to_string(file_path)
        .with_context(|| format!("无法读取航路文件: {}", file_path.display()))?;
    let pattern = Regex::new(r"P\d{2,3}")?;
    let mut routes = BTreeMap::new();
    let mut naip_route_idents = HashSet::new();

    for line in contents.lines() {
        let Some(record) = parse_route_record(line) else {
            continue;
        };
        if pattern.is_match(record.fix_ident) {
            naip_route_idents.insert(record.route_ident.to_string());
        }
        routes
            .entry(record.route_ident.to_string())
            .or_insert_with(Vec::new)
            .push(RouteLine {
                segment_number: record.segment_number.parse().unwrap_or_default(),
                fix_ident: record.fix_ident.to_string(),
                latitude: record.latitude.to_string(),
                longitude: record.longitude.to_string(),
            });
    }

    Ok(GeneratedRouteData {
        routes,
        naip_route_idents,
    })
}

struct ParsedRouteRecord<'a> {
    route_ident: &'a str,
    segment_number: &'a str,
    fix_ident: &'a str,
    latitude: &'a str,
    longitude: &'a str,
}

fn parse_route_record(line: &str) -> Option<ParsedRouteRecord<'_>> {
    if line.starts_with(';') || line.trim().is_empty() {
        return None;
    }
    let mut fields = line.split_ascii_whitespace();
    Some(ParsedRouteRecord {
        route_ident: fields.next()?,
        segment_number: fields.next()?,
        fix_ident: fields.next()?,
        latitude: fields.next()?,
        longitude: fields.next()?,
    })
}

fn write_generated_airway_segment(buffer: &mut String, segment: &GeneratedAirwaySegment) {
    let _ = writeln!(
        buffer,
        "{} {:03} {} {:.6} {:.6}",
        segment.route_ident,
        segment.segment_number,
        segment.fix_ident,
        segment.latitude,
        segment.longitude,
    );
}

fn write_route_line(buffer: &mut String, route_ident: &str, segment: &RouteLine) {
    let _ = writeln!(
        buffer,
        "{} {:03} {} {} {}",
        route_ident,
        segment.segment_number,
        segment.fix_ident,
        segment.latitude,
        segment.longitude,
    );
}

fn process_dicts(
    dict1: &BTreeMap<String, Vec<RouteLine>>,
    mut dict2: BTreeMap<String, Vec<RouteLine>>,
) -> BTreeMap<String, Vec<RouteLine>> {
    for (route_ident, left_segments) in dict1 {
        if let Some(right_segments) = dict2.get_mut(route_ident) {
            let left_first = left_segments
                .iter()
                .min_by_key(|segment| segment.segment_number)
                .map(|segment| segment.fix_ident.as_str());
            let right_first = right_segments
                .iter()
                .min_by_key(|segment| segment.segment_number)
                .map(|segment| segment.fix_ident.as_str());
            if left_first != right_first {
                let len = right_segments.len();
                for index in 0..(len / 2) {
                    let opposite = len - 1 - index;
                    let (left_slice, right_slice) = right_segments.split_at_mut(opposite);
                    let left = &mut left_slice[index];
                    let right = &mut right_slice[0];
                    std::mem::swap(&mut left.fix_ident, &mut right.fix_ident);
                    std::mem::swap(&mut left.latitude, &mut right.latitude);
                    std::mem::swap(&mut left.longitude, &mut right.longitude);
                }
            }
        }
    }
    dict2
}

fn filter_checked_routes(
    processed_dict: BTreeMap<String, Vec<RouteLine>>,
    naip_route_idents: &HashSet<String>,
) -> BTreeMap<String, Vec<RouteLine>> {
    let mut filtered = BTreeMap::new();
    for (route_ident, segments) in processed_dict {
        if route_ident.starts_with("XX") {
            continue;
        }
        if matches!(route_ident.chars().next(), Some('A' | 'B' | 'G' | 'L' | 'M' | 'R' | 'V' | 'W'))
            && !naip_route_idents.contains(&route_ident)
        {
            continue;
        }
        filtered.insert(route_ident, segments);
    }

    filtered
}

fn compare_and_insert(
    file1_dict: &mut BTreeMap<String, Vec<RouteLine>>,
    file2_dict: BTreeMap<String, Vec<RouteLine>>,
) {
    for (route_ident, file2_segments) in file2_dict {
        if file2_segments.is_empty() {
            continue;
        }

        if let Some(file1_segments) = file1_dict.get_mut(&route_ident) {
            let first_match = file1_segments
                .iter()
                .position(|segment| segment.fix_ident == file2_segments[0].fix_ident);
            let last_match = file1_segments
                .iter()
                .rposition(|segment| segment.fix_ident == file2_segments[file2_segments.len() - 1].fix_ident);

            if let (Some(start_index), Some(end_index)) = (first_match, last_match) {
                if start_index <= end_index {
                    file1_segments.splice(start_index..=end_index, file2_segments);
                } else {
                    file1_segments.splice(start_index..start_index, file2_segments);
                }
            } else {
                file1_segments.extend(file2_segments);
            }
        } else {
            file1_dict.insert(route_ident, file2_segments);
        }
    }
}

fn save_route_dict(file_path: &Path, dict: &BTreeMap<String, Vec<RouteLine>>) -> Result<()> {
    let estimated_line_count: usize = dict.values().map(Vec::len).sum();
    let mut output = String::with_capacity(estimated_line_count.saturating_mul(40));
    for (route_ident, segments) in dict {
        for segment in segments {
            write_route_line(&mut output, route_ident, segment);
        }
    }
    fs::write(file_path, output)?;
    Ok(())
}

fn save_sample_to_file(file_path: &Path) -> Result<()> {
    let sample_text = ";Supplemental Navaid Database (Option)\n;;\n;Data format is same as P3D_root\\iFly\\737MAX\\navdata\\\n;;\n;If any route in this file have same identifier as in\n;Main Navaid Database, FMC will delete route data in\n;the Main Navaid Database\n;;\n;This is a sample file\n;-------------------------------------------------------------\nTEST 001 TEST1 33.114350 139.788483\nTEST 002 TEST2 33.193211 138.972397\nTEST 003 TEST3 33.447742 135.794495\n";
    fs::write(file_path, sample_text)?;
    Ok(())
}