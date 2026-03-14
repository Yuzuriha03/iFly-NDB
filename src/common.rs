use std::ffi::OsStr;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use rusqlite::{types::ValueRef, Connection, Row};

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
        .trim_start_matches(|c| c == '\'' || c == '"' || c == '&' || c == ' ')
        .trim_end_matches(|c| c == '\'' || c == '"' || c == ' ')
        .to_string()
}

pub fn prompt_path(prompt: &str, expected_suffix: &str) -> Result<PathBuf> {
    loop {
        let raw = prompt_line(prompt)?;
        let path_text = sanitize_input_path(&raw);
        println!("{path_text}");
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
) -> Result<(PathBuf, PathBuf, Vec<PathBuf>)> {
    if let Some(route_file) = route_file {
        let navdata = navdata_path.unwrap_or_else(|| derive_navdata_path(&route_file));
        return Ok((route_file, navdata, Vec::new()));
    }

    if let Some(navdata) = navdata_path {
        let route = navdata.join("Permanent").join("WPNAVRTE.txt");
        return Ok((route, navdata, Vec::new()));
    }

    auto_detect_route_file().or_else(|_| {
        let route_file = prompt_path(
            "请输入iFly航路文件路径(位于Community\\ifly-aircraft-737max8\\Data\\navdata\\Permanent\\WPNAVRTE.txt)：",
            "WPNAVRTE.txt",
        )?;
        let navdata = derive_navdata_path(&route_file);
        Ok((route_file, navdata, Vec::new()))
    })
}

pub fn derive_navdata_path(route_file: &Path) -> PathBuf {
    route_file
        .parent()
        .and_then(Path::parent)
        .map(Path::to_path_buf)
        .unwrap_or_else(|| route_file.to_path_buf())
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

pub fn delete_data_navdatasupplemental(navdata_path: &Path) -> Result<()> {
    if let Some(parent) = navdata_path.parent() {
        let target = parent.join("navdataSupplemental");
        if target.exists() {
            let _ = fs::remove_dir_all(&target);
        }
    }
    Ok(())
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

pub fn sync_navdata_to_other_path(source_navdata: &Path, target_navdata: &Path, update_layout: bool) -> Result<()> {
    let sync_result: Result<()> = (|| {
        if target_navdata.exists() {
            fs::remove_dir_all(target_navdata)
                .with_context(|| format!("无法清理目标目录: {}", target_navdata.display()))?;
        }
        copy_dir_recursive(source_navdata, target_navdata)?;
        if update_layout {
            update_layout_json(target_navdata)?;
        }
        Ok(())
    })();

    if let Err(error) = sync_result {
        eprintln!("同步目录失败 {}: {error:#}", target_navdata.display());
    }
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
        ValueRef::Null => None,
        ValueRef::Real(value) => Some(value),
        ValueRef::Integer(value) => Some(value as f64),
        ValueRef::Text(value) => std::str::from_utf8(value).ok().and_then(|text| text.parse().ok()),
        ValueRef::Blob(_) => None,
    })
}

pub fn row_opt_i64(row: &Row<'_>, idx: usize) -> rusqlite::Result<Option<i64>> {
    let value = row.get_ref(idx)?;
    Ok(match value {
        ValueRef::Null => None,
        ValueRef::Integer(value) => Some(value),
        ValueRef::Real(value) => Some(value.round() as i64),
        ValueRef::Text(value) => std::str::from_utf8(value).ok().and_then(|text| text.parse().ok()),
        ValueRef::Blob(_) => None,
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
        ValueRef::Null => None,
        ValueRef::Integer(value) => Some(value.to_string()),
        ValueRef::Real(value) => Some(trimmed_float(value)),
        ValueRef::Text(value) => Some(String::from_utf8_lossy(value).to_string()),
        ValueRef::Blob(_) => None,
    }
}

fn auto_detect_route_file() -> Result<(PathBuf, PathBuf, Vec<PathBuf>)> {
    let checks = [
        (
            "MSFS2020 (Microsoft Store)",
            expand_env(r"%LocalAppData%\Packages\Microsoft.FlightSimulator_8wekyb3d8bbwe\LocalCache\UserCfg.opt"),
        ),
        (
            "MSFS2020 (Steam)",
            expand_env(r"%AppData%\Roaming\Microsoft Flight Simulator\UserCfg.opt"),
        ),
        (
            "MSFS2024 (Microsoft Store)",
            expand_env(r"%LocalAppData%\Packages\Microsoft.Limitless_8wekyb3d8bbwe\LocalCache\UserCfg.opt"),
        ),
        (
            "MSFS2024 (Steam)",
            expand_env(r"%AppData%\Roaming\Microsoft Flight Simulator 2024\UserCfg.opt"),
        ),
    ];

    let mut available_navdata = Vec::new();
    for (name, cfg_path) in checks {
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
            if route_file.exists() {
                available_navdata.push((name.to_string(), route_file));
            }
        }
    }

    if available_navdata.is_empty() {
        bail!("无法自动找到 iFly 航路文件目录");
    }

    if available_navdata.len() > 1 {
        println!("找到多个航路文件目录，将自动处理所有目录:");
        for (index, (name, route_file)) in available_navdata.iter().enumerate() {
            println!("{}: {} 目录 - {}", index + 1, name, route_file.display());
        }
    }

    let route_file = available_navdata[0].1.clone();
    let navdata_path = derive_navdata_path(&route_file);
    let other_paths = available_navdata
        .iter()
        .skip(1)
        .map(|(_, path)| derive_navdata_path(path))
        .collect();
    Ok((route_file, navdata_path, other_paths))
}

fn copy_dir_recursive(src: &Path, dst: &Path) -> Result<()> {
    fs::create_dir_all(dst).with_context(|| format!("无法创建目录: {}", dst.display()))?;
    for entry in fs::read_dir(src).with_context(|| format!("无法读取目录: {}", src.display()))? {
        let entry = entry?;
        let src_path = entry.path();
        let dst_path = dst.join(entry.file_name());
        if src_path.is_dir() {
            copy_dir_recursive(&src_path, &dst_path)?;
        } else {
            fs::copy(&src_path, &dst_path).with_context(|| {
                format!("无法复制 {} -> {}", src_path.display(), dst_path.display())
            })?;
        }
    }
    Ok(())
}

fn expand_env(input: &str) -> PathBuf {
    let mut output = input.to_string();
    for (name, value) in std::env::vars() {
        output = output.replace(&format!("%{name}%"), &value);
    }
    PathBuf::from(output)
}

fn extract_quoted_value(line: &str) -> Option<String> {
    let start = line.find('"')?;
    let rest = &line[start + 1..];
    let end = rest.find('"')?;
    Some(rest[..end].to_string())
}