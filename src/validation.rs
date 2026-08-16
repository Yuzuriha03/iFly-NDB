//! Structural and cycle validation for authoritative navigation datasets.

use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use rusqlite::Connection;
use serde_json::Value;

use crate::common;

#[derive(Debug)]
pub struct ValidationReport {
    cycle: String,
    airport_records: usize,
    runway_records: usize,
    navaid_records: usize,
    fix_records: usize,
    route_records: usize,
    procedure_files: usize,
}

impl fmt::Display for ValidationReport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "验证通过：AIRAC {}；机场 {}，跑道 {}，导航台/ILS {}，航路点 {}，航路段 {}，程序文件 {}",
            self.cycle,
            self.airport_records,
            self.runway_records,
            self.navaid_records,
            self.fix_records,
            self.route_records,
            self.procedure_files
        )
    }
}

/// Validate the selected Fenix database against an installed or unpacked iFly
/// `navdata` directory. `navdata_path` may also point directly at `Permanent`.
pub fn validate_reference_dataset(db_path: &Path, navdata_path: &Path) -> Result<ValidationReport> {
    let conn = common::open_fenix_connection(db_path)?;
    let navdata_root = common::normalize_navdata_root(navdata_path);
    let permanent = navdata_root.join("Permanent");
    if !permanent.is_dir() {
        bail!("缺少 iFly Permanent 目录: {}", permanent.display());
    }

    let cycle = source_cycle(&conn)?;
    validate_destination_cycle(&permanent, &cycle)?;

    let airport_records = validate_fixed_file(&permanent.join("AIRPORTS.dat"), 25, 25)?;
    let runway_records = validate_fixed_file(&permanent.join("WPNAVAPT.txt"), 74, 75)?;
    let navaid_records = validate_fixed_file(&permanent.join("WPNAVAID.txt"), 61, 62)?;
    let fix_records = validate_fixed_file(&permanent.join("WPNAVFIX.txt"), 50, 50)?;
    let _gls_records = validate_fixed_file(&permanent.join("WPNAVGLS.txt"), 1, 256)?;
    let route_records = validate_route_file(&permanent.join("WPNAVRTE.txt"))?;

    if fix_records == 0 || route_records == 0 {
        bail!("iFly fix/route 数据为空");
    }

    let procedure_files = count_procedure_files(&permanent)?;
    if procedure_files == 0 {
        bail!("iFly Permanent 中未找到程序文件");
    }

    Ok(ValidationReport {
        cycle,
        airport_records,
        runway_records,
        navaid_records,
        fix_records,
        route_records,
        procedure_files,
    })
}

/// Lightweight conversion preflight that allows supplemental additions while
/// rejecting a mixed AIRAC cycle.
pub fn validate_navdata_cycle(navdata_path: &Path, expected_cycle: &str) -> Result<()> {
    let navdata_root = common::normalize_navdata_root(navdata_path);
    let permanent = navdata_root.join("Permanent");
    if !permanent.is_dir() {
        bail!("缺少 iFly Permanent 目录: {}", permanent.display());
    }
    validate_destination_cycle(&permanent, expected_cycle)
}

fn source_cycle(conn: &Connection) -> Result<String> {
    let cycle: String = conn
        .query_row(
            "SELECT val FROM config WHERE key='CycleName' LIMIT 1",
            [],
            |row| row.get(0),
        )
        .context("Fenix config 缺少 CycleName")?;
    common::fenix_airac_cycle(&cycle)
}

fn validate_destination_cycle(permanent: &Path, expected_cycle: &str) -> Result<()> {
    let cycle_path = permanent.join("cycle.json");
    let cycle_json: Value = serde_json::from_str(
        &fs::read_to_string(&cycle_path)
            .with_context(|| format!("无法读取 {}", cycle_path.display()))?,
    )
    .with_context(|| format!("无法解析 {}", cycle_path.display()))?;
    let destination_cycle = cycle_json
        .get("cycle")
        .and_then(Value::as_str)
        .context("iFly cycle.json 缺少 cycle")?;
    if destination_cycle != expected_cycle {
        bail!("AIRAC 周期不一致：Fenix={expected_cycle}, iFly={destination_cycle}；拒绝混合数据");
    }

    let ident_path = permanent.join("FMC_Ident.txt");
    let ident = fs::read_to_string(&ident_path)
        .with_context(|| format!("无法读取 {}", ident_path.display()))?;
    if !ident.contains(&format!("NavData=AIRAC-{expected_cycle}")) {
        bail!("FMC_Ident.txt 与 AIRAC {expected_cycle} 不一致");
    }
    Ok(())
}

fn validate_fixed_file(path: &Path, min_width: usize, max_width: usize) -> Result<usize> {
    let bytes = fs::read(path).with_context(|| format!("无法读取 {}", path.display()))?;
    if bytes.windows(2).all(|window| window != b"\r\n") {
        bail!("{} 不是 CRLF 文本", path.display());
    }
    let mut records = 0usize;
    for (index, raw_line) in bytes.split(|byte| *byte == b'\n').enumerate() {
        let line = raw_line.strip_suffix(b"\r").unwrap_or(raw_line);
        if line.is_empty() || line.starts_with(b";") {
            continue;
        }
        if !line.is_ascii() || !(min_width..=max_width).contains(&line.len()) {
            bail!(
                "{} 第 {} 行格式错误：长度 {}，期望 {}..={} 个 ASCII 字节",
                path.display(),
                index + 1,
                line.len(),
                min_width,
                max_width
            );
        }
        records += 1;
    }
    if records == 0 {
        bail!("{} 没有数据记录", path.display());
    }
    Ok(records)
}

fn validate_route_file(path: &Path) -> Result<usize> {
    let contents =
        fs::read_to_string(path).with_context(|| format!("无法读取 {}", path.display()))?;
    let mut records = 0usize;
    for (index, line) in contents.lines().enumerate() {
        if line.is_empty() || line.starts_with(';') {
            continue;
        }
        let fields = line.split_whitespace().collect::<Vec<_>>();
        let valid = fields.len() == 5
            && fields[1].parse::<usize>().is_ok()
            && fields[3].parse::<f64>().is_ok()
            && fields[4].parse::<f64>().is_ok();
        if !valid {
            bail!("{} 第 {} 行不是有效航路记录", path.display(), index + 1);
        }
        records += 1;
    }
    Ok(records)
}

fn count_procedure_files(permanent: &Path) -> Result<usize> {
    let mut count = 0usize;
    for directory in [permanent.join("Sid"), permanent.join("Star")] {
        if !directory.is_dir() {
            bail!("缺少程序目录: {}", directory.display());
        }
        for entry in
            fs::read_dir(&directory).with_context(|| format!("无法读取 {}", directory.display()))?
        {
            let path: PathBuf = entry?.path();
            if path.is_file() {
                count += 1;
            }
        }
    }
    if !permanent.join("Supp").is_dir() {
        bail!("缺少机场补充目录: {}", permanent.join("Supp").display());
    }
    Ok(count)
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    use rusqlite::Connection;

    use super::{source_cycle, validate_reference_dataset};

    #[test]
    fn accepts_fenix_cycle_name_with_revision_suffix() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE config (key TEXT PRIMARY KEY, val TEXT);
             INSERT INTO config (key, val) VALUES ('CycleName', '2608n1');",
        )
        .unwrap();

        assert_eq!(source_cycle(&conn).unwrap(), "2608");
    }

    #[test]
    fn accepts_a_valid_ifly_baseline_with_different_vendor_record_counts() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "ifly_ndb_validation_vendor_count_mismatch_{}_{}",
            std::process::id(),
            unique
        ));
        let permanent = root.join("navdata/Permanent");
        fs::create_dir_all(permanent.join("Sid")).unwrap();
        fs::create_dir_all(permanent.join("Star")).unwrap();
        fs::create_dir_all(permanent.join("Supp")).unwrap();
        fs::write(
            permanent.join("cycle.json"),
            r#"{"cycle":"2608","revision":"1"}"#,
        )
        .unwrap();
        fs::write(
            permanent.join("FMC_Ident.txt"),
            "[Ident]\r\nNavData=AIRAC-2608\r\n",
        )
        .unwrap();
        fs::write(permanent.join("AIRPORTS.dat"), "A".repeat(25) + "\r\n").unwrap();
        fs::write(permanent.join("WPNAVAPT.txt"), "A".repeat(74) + "\r\n").unwrap();
        fs::write(permanent.join("WPNAVAID.txt"), "A".repeat(61) + "\r\n").unwrap();
        fs::write(permanent.join("WPNAVFIX.txt"), "A".repeat(50) + "\r\n").unwrap();
        fs::write(permanent.join("WPNAVGLS.txt"), "A\r\n").unwrap();
        fs::write(permanent.join("WPNAVRTE.txt"), "RTE 1 FIX 1.0 2.0\r\n").unwrap();
        fs::write(permanent.join("Sid/TEST.sid"), "[Procedure]\r\n").unwrap();

        let db_path = root.join("source.db3");
        let conn = Connection::open(&db_path).unwrap();
        conn.execute_batch(
            "CREATE TABLE config (key TEXT PRIMARY KEY, val TEXT);
             INSERT INTO config (key, val) VALUES ('CycleName', '2608n1');
             CREATE TABLE Airports (ID INTEGER);
             INSERT INTO Airports VALUES (1), (2);
             CREATE TABLE Runways (ID INTEGER);
             CREATE TABLE ILSes (ID INTEGER);
             CREATE TABLE Navaids (ID INTEGER);
             CREATE TABLE AirportCommunication (ID INTEGER);
             CREATE TABLE AirportLookup (ID INTEGER);
             CREATE TABLE AirwayLegs (ID INTEGER);
             CREATE TABLE Airways (ID INTEGER);
             CREATE TABLE Gls (ID INTEGER);
             CREATE TABLE GridMora (ID INTEGER);
             CREATE TABLE Holdings (ID INTEGER);
             CREATE TABLE Markers (ID INTEGER);
             CREATE TABLE MarkerTypes (ID INTEGER);
             CREATE TABLE NavaidLookup (ID INTEGER);
             CREATE TABLE NavaidTypes (ID INTEGER);
             CREATE TABLE SurfaceTypes (ID INTEGER);
             CREATE TABLE TerminalLegs (ID INTEGER);
             CREATE TABLE TerminalLegsEx (ID INTEGER);
             CREATE TABLE Terminals (ID INTEGER);
             CREATE TABLE TrmLegTypes (ID INTEGER);
             CREATE TABLE WaypointLookup (ID INTEGER);
             CREATE TABLE Waypoints (ID INTEGER);",
        )
        .unwrap();
        drop(conn);

        let result = validate_reference_dataset(&db_path, &root.join("navdata"));
        fs::remove_dir_all(root).unwrap();
        result.unwrap();
    }
}
