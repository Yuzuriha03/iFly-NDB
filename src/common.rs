use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use num_traits::ToPrimitive;
use rusqlite::{types::ValueRef, Connection, Row};

#[derive(Debug, Clone)]
pub struct NavdataTarget {
    pub source_label: String,
    pub route_file: PathBuf,
    pub navdata_path: PathBuf,
}

const REQUIRED_TABLES: &[&str] = &[
    "AirportCommunication",
    "AirportLookup",
    "Airports",
    "AirwayLegs",
    "Airways",
    "config",
    "Gls",
    "GridMora",
    "Holdings",
    "ILSes",
    "Markers",
    "MarkerTypes",
    "NavaidLookup",
    "Navaids",
    "NavaidTypes",
    "Runways",
    "SurfaceTypes",
    "TerminalLegs",
    "TerminalLegsEx",
    "Terminals",
    "TrmLegTypes",
    "WaypointLookup",
    "Waypoints",
];

pub fn prompt_line(prompt: &str) -> Result<String> {
    print!("{prompt}");
    io::stdout().flush().context("failed to flush stdout")?;

    let mut input = String::new();
    io::stdin()
        .read_line(&mut input)
        .context("failed to read input")?;
    Ok(input.trim().to_string())
}

pub fn sanitize_input_path(raw: &str) -> String {
    raw.trim()
    .trim_start_matches(['\'', '"', '&', ' '])
    .trim_end_matches(['\'', '"', ' '])
        .to_string()
}

pub fn prompt_path(prompt: &str, expected_suffix: &str) -> Result<PathBuf> {
    loop {
        let raw = prompt_line(prompt)?;
        let path_text = sanitize_input_path(&raw);
        println!("已输入路径：{path_text}");
        let path = PathBuf::from(&path_text);
        if path.exists() && path_text.ends_with(expected_suffix) {
            return Ok(path);
        }
        eprintln!("无效的文件路径或不是一个{expected_suffix}文件。请重新输入。");
    }
}

pub fn open_fenix_connection(path: &Path) -> Result<Connection> {
    if !path.exists() {
        bail!("数据库文件不存在: {}", path.display());
    }
    if path.extension() != Some(OsStr::new("db3")) {
        bail!("不是有效的 db3 文件: {}", path.display());
    }

    let conn = Connection::open(path)
        .with_context(|| format!("无法连接数据库: {}", path.display()))?;

    let mut tables = std::collections::HashSet::new();
    {
        let mut stmt = conn.prepare("SELECT name FROM sqlite_master WHERE type='table'")?;
        let table_iter = stmt.query_map([], |row| row.get::<_, String>(0))?;
        for table in table_iter {
            tables.insert(table?);
        }
    }

    let missing: Vec<&str> = REQUIRED_TABLES
        .iter()
        .copied()
        .filter(|name| !tables.contains(*name))
        .collect();
    if !missing.is_empty() {
        bail!("所读取文件不是Fenix数据库格式，缺少表: {}", missing.join(", "));
    }

    Ok(conn)
}

pub fn resolve_navdata_paths(
    route_file: Option<PathBuf>,
    navdata_path: Option<PathBuf>,
) -> Result<Vec<NavdataTarget>> {
    if let Some(route_file) = route_file {
        let navdata = navdata_path.unwrap_or_else(|| derive_navdata_path(&route_file));
        return Ok(vec![NavdataTarget {
            source_label: "手动指定".to_string(),
            route_file,
            navdata_path: navdata,
        }]);
    }

    if let Some(navdata) = navdata_path {
        let route = navdata.join("Permanent").join("WPNAVRTE.txt");
        return Ok(vec![NavdataTarget {
            source_label: "手动指定".to_string(),
            route_file: route,
            navdata_path: navdata,
        }]);
    }

    auto_detect_route_file().or_else(|_| {
        eprintln!("无法找到iFly航路文件目录，请手动指定路径：");
        let route_file = prompt_path(
            "请输入iFly航路文件路径(位于Community\\ifly-aircraft-737max8\\Data\\navdata\\Permanent\\WPNAVRTE.txt)：",
            "WPNAVRTE.txt",
        )?;
        let navdata = derive_navdata_path(&route_file);
        Ok(vec![NavdataTarget {
            source_label: "手动指定".to_string(),
            route_file,
            navdata_path: navdata,
        }])
    })
}

pub fn auto_detect_navdata_paths() -> Result<Vec<NavdataTarget>> {
    auto_detect_route_file()
}

pub fn announce_navdata_targets(targets: &[NavdataTarget]) {
    if targets.is_empty() {
        return;
    }

    if targets.len() == 1 {
        let target = &targets[0];
        println!(
            "检测到航路文件目录: {} 目录 - {}",
            target.source_label,
            target.route_file.display()
        );
        return;
    }

    println!("找到多个航路文件目录，将自动处理所有目录:");
    for (index, target) in targets.iter().enumerate() {
        println!("{}: {} 目录 - {}", index + 1, target.source_label, target.route_file.display());
    }
}

pub fn derive_navdata_path(route_file: &Path) -> PathBuf {
    route_file
        .parent()
        .and_then(Path::parent)
    .map_or_else(|| route_file.to_path_buf(), Path::to_path_buf)
}

pub fn resolve_terminal_range(
    start_terminal_id: Option<i64>,
    end_terminal_id: Option<i64>,
) -> Result<(i64, i64)> {
    match (start_terminal_id, end_terminal_id) {
        (Some(start), Some(end)) => Ok((start, end)),
        (Some(start), None) => Ok((start, 99_999_999)),
        (None, Some(_)) => bail!("提供结束 TerminalID 时也需要提供起始 TerminalID"),
        (None, None) => loop {
            let raw = prompt_line("请输入要转换终端程序集的起始TerminalID和结束TerminalID，用空格分隔二者：")?;
            let parts: Vec<&str> = raw.split_whitespace().collect();
            match parts.as_slice() {
                [start] if start.chars().all(|c| c.is_ascii_digit()) => {
                    println!("终止ID未输入，将自动转换到数据库中最后一个终端程序");
                    return Ok((start.parse()?, 99_999_999));
                }
                [start, end]
                    if start.chars().all(|c| c.is_ascii_digit())
                        && end.chars().all(|c| c.is_ascii_digit()) =>
                {
                    return Ok((start.parse()?, end.parse()?));
                }
                _ => eprintln!("请输入有效的数字，并用空格分隔！"),
            }
        },
    }
}

pub fn delete_data_navdatasupplemental(navdata_path: &Path) {
    if let Some(parent) = navdata_path.parent() {
        let target = parent.join("navdataSupplemental");
        if target.exists() {
            let _ = fs::remove_dir_all(&target);
        }
    }
}

pub fn update_layout_json(navdata_path: &Path) -> Result<()> {
    let package_root = navdata_path
        .parent()
        .and_then(Path::parent)
        .ok_or_else(|| anyhow!("无法推导 layout.json 所在目录"))?;
    let layout_json_path = package_root.join("layout.json");
    if !layout_json_path.exists() {
        eprintln!("未找到 layout.json 文件: {}", layout_json_path.display());
        return Ok(());
    }

    crate::layout::update_layout_json(&layout_json_path)?;
    Ok(())
}

pub fn countdown_timer(seconds: u64) {
    let mut remaining = seconds;
    while remaining > 0 {
        print!("处理结束，程序将在 {remaining} 秒钟后关闭\r");
        let _ = io::stdout().flush();
        thread::sleep(Duration::from_secs(1));
        remaining -= 1;
    }
    println!();
}

pub fn to_crlf(input: &str) -> String {
    input.replace("\r\n", "\n").replace('\n', "\r\n")
}

pub fn write_text_file(path: &Path, contents: &str) -> Result<()> {
    fs::write(path, to_crlf(contents))
        .with_context(|| format!("无法写入 {}", path.display()))?;
    Ok(())
}

pub fn row_opt_string(row: &Row<'_>, idx: usize) -> rusqlite::Result<Option<String>> {
    let value = row.get_ref(idx)?;
    Ok(value_ref_to_string(value))
}

pub fn row_string(row: &Row<'_>, idx: usize) -> rusqlite::Result<String> {
    Ok(row_opt_string(row, idx)?.unwrap_or_default())
}

pub fn row_opt_f64(row: &Row<'_>, idx: usize) -> rusqlite::Result<Option<f64>> {
    let value = row.get_ref(idx)?;
    Ok(match value {
        ValueRef::Real(value) => Some(value),
        ValueRef::Integer(value) => value.to_f64(),
        ValueRef::Text(value) => std::str::from_utf8(value).ok().and_then(|text| text.parse().ok()),
        ValueRef::Null | ValueRef::Blob(_) => None,
    })
}

pub fn row_opt_i64(row: &Row<'_>, idx: usize) -> rusqlite::Result<Option<i64>> {
    let value = row.get_ref(idx)?;
    Ok(match value {
        ValueRef::Integer(value) => Some(value),
        ValueRef::Real(value) => value.round().to_i64(),
        ValueRef::Text(value) => std::str::from_utf8(value).ok().and_then(|text| text.parse().ok()),
        ValueRef::Null | ValueRef::Blob(_) => None,
    })
}

pub fn opt_string_from_f64(value: Option<f64>) -> Option<String> {
    value.map(trimmed_float)
}

pub fn trimmed_float(value: f64) -> String {
    if value.fract() == 0.0 {
        format!("{value:.1}")
    } else {
        value.to_string()
    }
}

fn value_ref_to_string(value: ValueRef<'_>) -> Option<String> {
    match value {
        ValueRef::Integer(value) => Some(value.to_string()),
        ValueRef::Real(value) => Some(trimmed_float(value)),
        ValueRef::Text(value) => Some(String::from_utf8_lossy(value).to_string()),
        ValueRef::Null | ValueRef::Blob(_) => None,
    }
}

fn auto_detect_route_file() -> Result<Vec<NavdataTarget>> {
    let paths_to_check = [
        (
            "MSFS2020 (Microsoft Store)",
            expand_env(r"%LocalAppData%\Packages\Microsoft.FlightSimulator_8wekyb3d8bbwe\LocalCache\UserCfg.opt"),
        ),
        (
            "MSFS2020 (Steam)",
            expand_env(r"%AppData%\Roaming\Microsoft Flight Simulator\UserCfg.opt"),
        ),
    ];

    let mut route_files = vec![
        (
            "MSFS2024 (Microsoft Store)".to_string(),
            expand_env(r"%LocalAppData%\Packages\Microsoft.Limitless_8wekyb3d8bbwe\LocalState\WASM\MSFS2020\ifly-aircraft-737max8\work\navdata\Permanent\WPNAVRTE.txt"),
        ),
        (
            "MSFS2024 (Steam)".to_string(),
            expand_env(r"%AppData%\Microsoft Flight Simulator 2024\WASM\MSFS2020\ifly-aircraft-737max8\work\navdata\Permanent\WPNAVRTE.txt"),
        ),
    ];

    for (name, cfg_path) in paths_to_check {
        if !cfg_path.exists() {
            continue;
        }

        let contents = fs::read_to_string(&cfg_path)
            .with_context(|| format!("无法读取 {}", cfg_path.display()))?;
        if let Some(installed_packages_path) = contents
            .lines()
            .find(|line| line.contains("InstalledPackagesPath"))
            .and_then(extract_quoted_value)
        {
            let route_file = PathBuf::from(installed_packages_path)
                .join("Community")
                .join("ifly-aircraft-737max8")
                .join("Data")
                .join("navdata")
                .join("Permanent")
                .join("WPNAVRTE.txt");
            route_files.push((name.to_string(), route_file));
        }
    }

    if route_files.is_empty() {
        bail!("无法自动找到 iFly 航路文件目录");
    }

    let available_files: Vec<(String, PathBuf)> = route_files
        .into_iter()
        .filter(|(_, route_file)| route_file.exists())
        .collect();

    if available_files.is_empty() {
        bail!("无法自动找到 iFly 航路文件目录");
    }

    Ok(available_files
        .into_iter()
        .map(|(source_label, route_file)| NavdataTarget {
            source_label,
            navdata_path: derive_navdata_path(&route_file),
            route_file,
        })
        .collect())
}

fn expand_env(input: &str) -> PathBuf {
    let env_map: HashMap<String, String> = std::env::vars()
        .map(|(name, value)| (name.to_ascii_lowercase(), value))
        .collect();

    let mut output = String::with_capacity(input.len());
    let mut remaining = input;

    while let Some(start) = remaining.find('%') {
        output.push_str(&remaining[..start]);
        let placeholder = &remaining[start + 1..];
        let Some(end) = placeholder.find('%') else {
            output.push_str(&remaining[start..]);
            return PathBuf::from(output);
        };

        let variable_name = &placeholder[..end];
        if let Some(value) = env_map.get(&variable_name.to_ascii_lowercase()) {
            output.push_str(value);
        } else {
            output.push('%');
            output.push_str(variable_name);
            output.push('%');
        }

        remaining = &placeholder[end + 1..];
    }

    output.push_str(remaining);
    PathBuf::from(output)
}

fn extract_quoted_value(line: &str) -> Option<String> {
    let start = line.find('"')?;
    let rest = &line[start + 1..];
    let end = rest.find('"')?;
    Some(rest[..end].to_string())
}