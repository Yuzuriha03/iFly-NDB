//! Deterministic post-conversion altitude corrections for terminal legs.

use std::fs;
use std::path::{Component, Path, PathBuf};

use anyhow::{bail, Context, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
struct AltitudeOverride {
    relative_file: PathBuf,
    section: String,
    altitude: String,
}

/// Apply a UTF-8 text file whose non-comment rows use:
///
/// `relative_procedure_file|section_header_without_brackets|altitude`
///
/// Example: `SID/ZWTK.sid|DSC3Z.33.1|6000`
pub fn apply_file(navdata_path: &Path, override_path: &Path) -> Result<usize> {
    let contents = fs::read_to_string(override_path)
        .with_context(|| format!("无法读取高度修正规则 {}", override_path.display()))?;
    let overrides = parse_overrides(&contents)?;
    let supplemental_root = navdata_path.join("Supplemental");

    for rule in &overrides {
        let target = supplemental_root.join(&rule.relative_file);
        if !target.exists() {
            bail!("高度修正规则目标不存在: {}", target.display());
        }
        let procedure = fs::read_to_string(&target)
            .with_context(|| format!("无法读取程序文件 {}", target.display()))?;
        let updated = apply_one(&procedure, &rule.section, &rule.altitude)
            .with_context(|| format!("无法在 {} 找到段 [{}]", target.display(), rule.section))?;
        crate::common::write_text_file(&target, &updated)?;
    }

    Ok(overrides.len())
}

fn parse_overrides(contents: &str) -> Result<Vec<AltitudeOverride>> {
    let mut overrides = Vec::new();
    for (index, raw_line) in contents.lines().enumerate() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let mut fields = line.split('|').map(str::trim);
        let file = fields.next().unwrap_or_default();
        let section = fields.next().unwrap_or_default();
        let altitude = fields.next().unwrap_or_default();
        if fields.next().is_some() || file.is_empty() || section.is_empty() || altitude.is_empty() {
            bail!("高度修正规则第 {} 行格式无效，应为 文件|段|高度", index + 1);
        }
        let relative_file = PathBuf::from(file);
        if relative_file.is_absolute()
            || relative_file.components().any(|part| {
                matches!(
                    part,
                    Component::ParentDir | Component::RootDir | Component::Prefix(_)
                )
            })
        {
            bail!("高度修正规则第 {} 行包含无效文件路径", index + 1);
        }
        if !altitude.chars().all(|character| {
            character.is_ascii_digit() || matches!(character, '-' | '+' | '.' | '/' | 'A'..='Z')
        }) {
            bail!("高度修正规则第 {} 行高度值无效", index + 1);
        }
        overrides.push(AltitudeOverride {
            relative_file,
            section: section.trim_matches(['[', ']']).to_string(),
            altitude: altitude.to_string(),
        });
    }
    Ok(overrides)
}

fn apply_one(contents: &str, section: &str, altitude: &str) -> Result<String> {
    let header = format!("[{section}]");
    let mut lines: Vec<String> = contents.lines().map(str::to_string).collect();
    let start = lines
        .iter()
        .position(|line| line.trim() == header)
        .context("未找到段")?;
    let end = lines[start + 1..]
        .iter()
        .position(|line| line.trim_start().starts_with('['))
        .map_or(lines.len(), |relative| start + 1 + relative);
    if let Some(index) = (start + 1..end).find(|index| lines[*index].starts_with("Altitude=")) {
        lines[index] = format!("Altitude={altitude}");
    } else {
        lines.insert(start + 1, format!("Altitude={altitude}"));
    }
    Ok(lines.join("\n") + "\n")
}

#[cfg(test)]
mod tests {
    use super::{apply_one, parse_overrides};

    #[test]
    fn parses_and_rejects_unsafe_override_paths() {
        assert_eq!(
            parse_overrides("SID/ZWTK.sid|DSC3Z.33.1|6000\n")
                .unwrap()
                .len(),
            1
        );
        assert!(parse_overrides("../SID/ZWTK.sid|DSC3Z.33.1|6000").is_err());
    }

    #[test]
    fn inserts_or_replaces_altitude_in_exact_section() {
        let source = "[A.01.0]\nLeg=TF\n\n[B.01.0]\nAltitude=3000\n";
        let inserted = apply_one(source, "A.01.0", "5000").unwrap();
        assert!(inserted.contains("[A.01.0]\nAltitude=5000\nLeg=TF"));
        let replaced = apply_one(&inserted, "B.01.0", "4000").unwrap();
        assert!(replaced.contains("[B.01.0]\nAltitude=4000"));
    }
}
