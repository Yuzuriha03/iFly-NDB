use std::fs;
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use serde::Serialize;
use serde_json::{Map, Value};
use walkdir::WalkDir;

const WINDOWS_TICK: u64 = 10_000_000;
const WINDOWS_EPOCH_OFFSET_SECS: u64 = 11_644_473_600;

#[derive(Debug, Serialize)]
struct Layout {
    content: Vec<Content>,
}

#[derive(Debug, Serialize)]
struct Content {
    path: String,
    size: u64,
    date: u64,
}

pub fn update_layout_json(layout_json_path: &Path) -> Result<()> {
    if !layout_json_path
        .file_name()
        .is_some_and(|name| name.eq_ignore_ascii_case("layout.json"))
    {
        eprintln!(
            "文件名不是 layout.json，已跳过: {}",
            layout_json_path.display()
        );
        return Ok(());
    }

    let package_root = layout_json_path
        .parent()
        .context("无法定位 layout.json 所在目录")?;

    let mut content_entries = Vec::new();
    let mut total_package_size = 0u64;

    for entry in WalkDir::new(package_root)
        .into_iter()
        .filter_entry(|entry| !is_ignored_dir(entry.path(), package_root))
    {
        let entry = entry.with_context(|| {
            format!("无法遍历目录 {}", package_root.display())
        })?;
        if !entry.file_type().is_file() {
            continue;
        }

        let file_path = entry.path();
        let relative_path = to_layout_relative_path(file_path, package_root)?;
        let metadata = entry
            .metadata()
            .with_context(|| format!("无法读取文件元数据: {}", file_path.display()))?;
        let file_size = metadata.len();

        if !should_omit_from_layout(&relative_path) {
            content_entries.push(Content {
                path: relative_path.clone(),
                size: file_size,
                date: system_time_to_filetime(
                    metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH),
                ),
            });
            total_package_size += file_size;
        }

        if relative_path.eq_ignore_ascii_case("manifest.json") {
            total_package_size += file_size;
        }
    }

    if content_entries.is_empty() {
        eprintln!(
            "在 {} 所在目录下未找到可写入 layout.json 的文件，已跳过更新。",
            layout_json_path.display()
        );
        return Ok(());
    }

    let layout = Layout {
        content: content_entries,
    };
    let layout_json = serde_json::to_string_pretty(&layout).context("无法序列化 layout.json")?;
    fs::write(layout_json_path, normalize_lf(&layout_json))
        .with_context(|| format!("无法写入 {}", layout_json_path.display()))?;

    total_package_size += fs::metadata(layout_json_path)
        .with_context(|| format!("无法读取 {}", layout_json_path.display()))?
        .len();

    let manifest_path = package_root.join("manifest.json");
    if manifest_path.exists() {
        update_manifest_json(&manifest_path, total_package_size)?;
    }

    Ok(())
}

fn update_manifest_json(manifest_path: &Path, total_package_size: u64) -> Result<()> {
    let manifest_text = fs::read_to_string(manifest_path)
        .with_context(|| format!("无法读取 {}", manifest_path.display()))?;
    let mut manifest_value: Value = serde_json::from_str(&manifest_text)
        .with_context(|| format!("无法解析 {}", manifest_path.display()))?;

    if let Some(object) = manifest_value.as_object_mut() {
        if object.contains_key("total_package_size") {
            object.insert(
                "total_package_size".to_string(),
                Value::String(format!("{total_package_size:020}")),
            );
            let manifest_json = serialize_manifest(object)?;
            fs::write(manifest_path, to_crlf(&manifest_json))
                .with_context(|| format!("无法写入 {}", manifest_path.display()))?;
        }
    }

    Ok(())
}

fn serialize_manifest(object: &Map<String, Value>) -> Result<String> {
    serde_json::to_string_pretty(&Value::Object(object.clone())).context("无法序列化 manifest.json")
}

fn to_layout_relative_path(path: &Path, package_root: &Path) -> Result<String> {
    let relative = path
        .strip_prefix(package_root)
        .with_context(|| format!("无法计算相对路径: {}", path.display()))?;
    Ok(relative
        .components()
        .map(|component| component.as_os_str().to_string_lossy().into_owned())
        .collect::<Vec<_>>()
        .join("/"))
}

fn should_omit_from_layout(relative_path: &str) -> bool {
    relative_path.starts_with("_CVT_")
        || relative_path.eq_ignore_ascii_case("layout.json")
        || relative_path.eq_ignore_ascii_case("manifest.json")
        || relative_path.eq_ignore_ascii_case("MSFSLayoutGenerator.exe")
}

fn is_ignored_dir(path: &Path, package_root: &Path) -> bool {
    if path == package_root {
        return false;
    }

    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.eq_ignore_ascii_case("_CVT_"))
}

fn system_time_to_filetime(time: SystemTime) -> u64 {
    let duration = match time.duration_since(UNIX_EPOCH) {
        Ok(duration) => duration,
        Err(_) => Duration::ZERO,
    };
    (duration.as_secs() + WINDOWS_EPOCH_OFFSET_SECS) * WINDOWS_TICK
        + u64::from(duration.subsec_nanos() / 100)
}

fn normalize_lf(input: &str) -> String {
    input.replace("\r\n", "\n")
}

fn to_crlf(input: &str) -> String {
    input.replace("\r\n", "\n").replace('\n', "\r\n")
}

#[cfg(test)]
mod tests {
    use super::update_layout_json;
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    use serde_json::Value;

    #[test]
    fn updates_layout_and_manifest_without_external_exe() {
        let temp_root = std::env::temp_dir().join(format!(
            "ifly_layout_test_{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock went backwards")
                .as_nanos()
        ));
        let result = run_layout_update_test(&temp_root);
        let _ = fs::remove_dir_all(&temp_root);
        result.expect("layout update test failed");
    }

    fn run_layout_update_test(temp_root: &PathBuf) -> anyhow::Result<()> {
        fs::create_dir_all(temp_root.join("SimObjects"))?;
        fs::create_dir_all(temp_root.join("_CVT_"))?;

        let layout_path = temp_root.join("layout.json");
        let manifest_path = temp_root.join("manifest.json");
        let payload_path = temp_root.join("SimObjects").join("aircraft.cfg");
        let cvt_path = temp_root.join("_CVT_").join("cache.bin");

        fs::write(&layout_path, "{}")?;
        fs::write(
            &manifest_path,
            "{\n  \"package_version\": \"1.0.0\",\n  \"total_package_size\": \"00000000000000000000\"\n}",
        )?;
        fs::write(&payload_path, "payload")?;
        fs::write(&cvt_path, "ignore me")?;

        update_layout_json(&layout_path)?;

        let layout_value: Value = serde_json::from_str(&fs::read_to_string(&layout_path)?)?;
        let content = layout_value
            .get("content")
            .and_then(Value::as_array)
            .expect("layout content missing");
        assert_eq!(content.len(), 1);
        assert_eq!(content[0].get("path").and_then(Value::as_str), Some("SimObjects/aircraft.cfg"));

        let manifest_text = fs::read_to_string(&manifest_path)?;
        assert!(manifest_text.contains("\r\n"));
        let manifest_value: Value = serde_json::from_str(&manifest_text)?;
        let total_package_size = manifest_value
            .get("total_package_size")
            .and_then(Value::as_str)
            .expect("missing total_package_size");
        assert_eq!(total_package_size.len(), 20);
        assert_ne!(total_package_size, "00000000000000000000");

        Ok(())
    }
}