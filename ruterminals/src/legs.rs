use chrono::prelude::*;
use chrono_tz::Asia::Shanghai;
use regex::Regex;
use rayon::prelude::*;
use rusqlite::Connection;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::Instant;
use walkdir::WalkDir;

// 引入 pyo3，用于将函数暴露给 Python
use pyo3::prelude::*;
use pyo3::wrap_pyfunction;

// 从外部模块 list 导入 list_generate（该模块在 src/list.rs 中实现）
pub mod list; // 确保 src/list.rs 存在
pub mod merged_data;

/// 用于表示每一行数据，相当于 pandas DataFrame 中的一行
#[derive(Clone, Debug)]
pub struct Row {
    pub icao: String,
    pub type_: String,
    pub transition: String,
    pub terminal: String,
    pub rwy: String,
    /// 其他非关键列，键为列名，值为内容
    pub extras: HashMap<String, String>,
}

/// 解析给定文件，提取流程和细节信息。若文件后缀不在指定范围内，则返回空字典。
pub fn parse_files(file: &str, root: &Path) -> (HashMap<String, Vec<String>>, HashMap<String, Vec<String>>) {
    let mut procedures: HashMap<String, Vec<String>> = HashMap::new();
    let mut details: HashMap<String, Vec<String>> = HashMap::new();

    let allowed_extensions = [".app", ".apptrs", ".sid", ".sidtrs", ".star"];
    if !allowed_extensions.iter().any(|ext| file.ends_with(ext)) {
        return (procedures, details);
    }

    let icao = file.split('.').next().unwrap_or("").to_string();
    let file_path = root.join(file);
    let content = fs::read_to_string(&file_path).unwrap_or_default();
    let lines: Vec<&str> = content.lines().collect();

    let mut proc_vec: Vec<String> = Vec::new();
    let mut detail_set: HashSet<String> = HashSet::new();
    let mut list_started = false;

    let re_proc = Regex::new(r"Procedure\.(\d+)=(\S+)\.(\S+)").unwrap();
    let re_detail = Regex::new(r"\[(\S+)\.(\S+)\.(\d+)\]").unwrap();

    // 解析 [list] 部分，获取流程信息
    for line in &lines {
        if line.starts_with("[list]") {
            list_started = true;
        } else if list_started && !line.starts_with("[") {
            if let Some(caps) = re_proc.captures(line) {
                let transition = caps.get(2).unwrap().as_str();
                let via = caps.get(3).unwrap().as_str();
                proc_vec.push(format!("{}.{}.{}", icao, transition, via));
            }
        } else if list_started && line.starts_with("[") {
            break;
        }
    }
    procedures.insert(icao.clone(), proc_vec);

    // 解析所有以 [ 开头且以 ] 结尾的行，获取细节信息
    for line in &lines {
        if line.starts_with("[") && line.ends_with("]") {
            if let Some(caps) = re_detail.captures(line) {
                let transition = caps.get(1).unwrap().as_str();
                let via = caps.get(2).unwrap().as_str();
                detail_set.insert(format!("{}.{}", transition, via));
            }
        }
    }
    details.insert(icao, detail_set.into_iter().collect());

    (procedures, details)
}

/// 根据给定的 ICAO、流程信息、细节信息和数据，生成新的航段字符串
pub fn legs_generate(
    icao: &str,
    procedures: &HashMap<String, Vec<String>>,
    details: &HashMap<String, Vec<String>>,
    data: &[Row],
) -> Vec<String> {
    let mut results = Vec::new();
    let mut current_transition: Option<String> = None;
    let mut current_via: Option<String> = None;
    let mut seqno = 0;

    for row in data {
        if row.icao == icao {
            let (transition, via) = if row.type_ == "6" || row.type_ == "A" {
                (row.transition.clone(), row.terminal.clone())
            } else {
                // 跑道号补零，保证两位数字
                let via_padded = format!("{:0>2}", row.rwy);
                (row.terminal.clone(), via_padded)
            };
            let procedure = format!("{}.{}.{}", row.icao, transition, via);
            let name = format!("{}.{}", transition, via);

            if let Some(proc_list) = procedures.get(&row.icao) {
                if proc_list.contains(&procedure) {
                    if let Some(detail_list) = details.get(&row.icao) {
                        if !detail_list.contains(&name) {
                            // 如果 Terminal 或 Rwy 更新，重置 seqno，否则递增
                            if current_transition.as_deref() != Some(&transition) || current_via.as_deref() != Some(&via) {
                                current_transition = Some(transition.clone());
                                current_via = Some(via.clone());
                                seqno = 0;
                            } else {
                                seqno += 1;
                            }
                            let mut row_str = format!("[{}.{}.{}]\n", transition, via, seqno);
                            // 遍历 extras 中存储的其他数据
                            for (col, value) in &row.extras {
                                if !value.trim().is_empty() {
                                    row_str.push_str(&format!("{}={}\n", col, value));
                                }
                            }
                            results.push(row_str.trim().to_string());
                        }
                    }
                }
            }
        }
    }
    results
}

/// 处理单个文件：解析文件内容，生成新行并追加到文件末尾
pub fn process_file(file: &str, root: &Path, data: &[Row]) -> io::Result<()> {
    let icao = file.split('.').next().unwrap_or("");
    let (procedures, details) = parse_files(file, root);
    let results = legs_generate(icao, &procedures, &details, data);
    let filepath = root.join(file);

    let mut content = fs::read_to_string(&filepath)?;
    content.push('\n');
    for result in results {
        content.push_str(&result);
        content.push('\n');
    }
    fs::write(&filepath, content)?;
    Ok(())
}

/// 如果目标文件不存在，则复制源文件到目标位置（必要时创建目录）
pub fn copy_file_if_not_exists(src_file: &Path, dest_file: &Path) -> io::Result<()> {
    if dest_file.exists() {
        return Ok(());
    }
    if let Some(parent) = dest_file.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::copy(src_file, dest_file)?;
    Ok(())
}

/// 遍历指定目录下的文件，根据 ICAO 前缀及后缀条件将文件复制到 Supplemental 目录中
pub fn process_files(
    root: &Path,
    files: &[String],
    permanent_path: &Path,
    supplemental_path_base: &Path,
) -> io::Result<()> {
    let icao_prefixes = ["VQPR", "ZB", "ZG", "ZH", "ZJ", "ZL", "ZP", "ZS", "ZU", "ZW", "ZY"];
    let allowed_extensions = [".sid", ".sidtrs", ".app", ".apptrs", ".star", ".startrs"];
    for file in files {
        if icao_prefixes.iter().any(|prefix| file.starts_with(prefix))
            && allowed_extensions.iter().any(|ext| file.ends_with(ext))
        {
            let file_path = root.join(file);
            let relative_path = file_path.strip_prefix(permanent_path).unwrap_or(&file_path);
            let supplemental_path = supplemental_path_base.join(relative_path);
            copy_file_if_not_exists(&file_path, &supplemental_path)?;
        }
    }
    Ok(())
}

/// 主逻辑：
/// 1. 遍历 Permanent 目录，将符合条件的文件复制到 Supplemental 目录下；
/// 2. 调用 list::list_generate 获取数据；
/// 3. 遍历 Supplemental 目录，对每个文件调用 process_file 进行处理；
/// 4. 根据当前日期生成 FMC_Ident.txt 文件；
/// 5. 输出转换用时。
pub fn terminals(
    conn: Connection,
    navdata_path: &Path,
    start_terminal_id: i32,
    end_terminal_id: i32,
) -> io::Result<()> {
    let start_time = Instant::now();
    let permanent_path = navdata_path.join("Permanent");
    let supplemental_path_base = navdata_path.join("Supplemental");

    // 将 Permanent 目录下的数据复制到 Supplemental 目录下（并行处理）
    {
        let dirs: Vec<PathBuf> = WalkDir::new(&permanent_path)
            .into_iter()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_type().is_dir())
            .map(|e| e.path().to_path_buf())
            .collect();

        dirs.par_iter().try_for_each(|dir| -> io::Result<()> {
            let mut files = Vec::new();
            for entry in fs::read_dir(dir)? {
                let entry = entry?;
                if entry.file_type()?.is_file() {
                    files.push(entry.file_name().to_string_lossy().to_string());
                }
            }
            process_files(dir, &files, &permanent_path, &supplemental_path_base)
        })?;
    }

// 调用 list_generate 获取 merged_data 数据（正确处理 Result 和类型转换）
let merged_data = list::list_generate(
    conn, 
    start_terminal_id, 
    end_terminal_id, 
    navdata_path.to_str().unwrap()
)
.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("生成合并数据失败: {}", e)))?;

// 转换为 Row 结构（包含完整的字段处理）
let rows: Vec<Row> = merged_data
    .into_iter()
    .map(|record| {
        // 类型转换和默认值处理
        Row {
            icao: record.icao,
            type_: record.type_field
                .map(|v| v.to_string())    // 将 Option<i32> 转换为 Option<String>
                .unwrap_or_default(),     // 默认空字符串
            transition: record.transition
                .unwrap_or_else(|| "DEFAULT_TRANSITION".to_string()),
            terminal: record.terminal,
            rwy: record.rwy
                .unwrap_or_else(|| "00".to_string()),  // 默认跑道号 00
            extras: {
                // 构建 extras 字段（示例：包含其他数据）
                let mut map = HashMap::new();
                if let Some(leg) = record.leg {
                    map.insert("leg".to_string(), leg);
                }
                if let Some(turn) = record.turn_direction {
                    map.insert("turn_direction".to_string(), turn);
                }
                map
            },
        }
    })
    .collect();

    // 遍历 Supplemental 目录下的文件，并对每个文件进行处理（并行处理）
    {
        let supplemental_files: Vec<(PathBuf, String)> = WalkDir::new(&supplemental_path_base)
            .into_iter()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_type().is_file())
            .map(|e| {
                let dir = e.path().parent().unwrap().to_path_buf();
                let file_name = e.file_name().to_string_lossy().to_string();
                (dir, file_name)
            })
            .collect();

        supplemental_files.par_iter().try_for_each(|(dir, file)| {
            process_file(file, dir, &rows)
        })?;
    }

    // 生成 FMC_Ident.txt 文件
    let revision_table = vec![
        (2501, "2025-1-23"),
        (2502, "2025-2-20"),
        (2503, "2025-3-20"),
        (2504, "2025-4-17"),
        (2505, "2025-5-15"),
        (2506, "2025-6-12"),
        (2507, "2025-7-10"),
        (2508, "2025-8-7"),
        (2509, "2025-9-4"),
        (2510, "2025-10-2"),
        (2511, "2025-10-30"),
        (2512, "2025-11-27"),
        (2513, "2025-12-25"),
    ];

    let current_date = Utc::now().with_timezone(&Shanghai).date_naive();
    let mut matched_rev_code = 2501;
    for (rev_code, eff_date_str) in revision_table {
        let eff_date = NaiveDate::parse_from_str(eff_date_str, "%Y-%m-%d").unwrap();
        if current_date >= eff_date {
            matched_rev_code = rev_code;
        }
    }

    let fmc_ident_path = supplemental_path_base.join("FMC_Ident.txt");
    let fmc_content = format!("[Ident]\nSuppData=NAIP-{}\n", matched_rev_code);
    fs::write(&fmc_ident_path, fmc_content)?;

    let run_time = start_time.elapsed().as_secs_f64();
    println!("终端数据转换完毕，用时：{:.3}秒", run_time);

    Ok(())
}

/// 供 Python 调用的入口函数，使用 pyo3 封装
#[pyfunction]
fn py_terminals(
    db_path: &str,
    navdata_path: &str,
    start_terminal_id: i32,
    end_terminal_id: i32,
) -> PyResult<()> {
    let conn = Connection::open(db_path)
        .map_err(|e| pyo3::exceptions::PyException::new_err(format!("DB error: {}", e)))?;
    let navdata = Path::new(navdata_path);
    terminals(conn, navdata, start_terminal_id, end_terminal_id)
        .map_err(|e| pyo3::exceptions::PyException::new_err(format!("Terminals error: {}", e)))?;
    Ok(())
}

/// 定义 Python 模块，模块名称为 ruterminals
#[pymodule]
fn ruterminals(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(py_terminals, m)?)?;
    Ok(())
}
