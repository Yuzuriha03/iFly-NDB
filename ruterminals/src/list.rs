use regex::Regex;
use rusqlite::{params, Connection};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::sync::Arc;
use std::thread;

use crate::merged_data::{generate_merged_data, MergedDataRow as MergedRecord};

// 用于存储 Terminals 表记录（及 generate_transitions 生成的记录）
#[derive(Clone, Debug)]
struct TerminalRecord {
    proc: i32,
    icao: String,
    name: String,
    rwy: Option<String>,
}

/// 生成符合条件的 transitions（Type 为 "6" 或 "A"）
fn generate_transitions(data: &[MergedRecord]) -> Vec<TerminalRecord> {
    let mut transitions = Vec::new();
    for row in data {
        let type_str = row.type_field.map(|v| v.to_string()).unwrap_or_default();
        if type_str == "6" || type_str == "A" {
            transitions.push(TerminalRecord {
                proc: type_str.parse().unwrap_or_default(),
                icao: row.icao.clone(),
                name: row.transition.clone().unwrap_or_default(),
                rwy: row.rwy.clone(),
            });
        }
    }
    transitions
}

/// 获取 Terminals 数据，并处理、合并与扩展
fn get_terminals(
    conn: Connection,
    start_terminal_id: i32,
    end_terminal_id: i32,
    navdata_path: &str,
) -> Result<(Vec<TerminalRecord>, Vec<MergedRecord>), Box<dyn std::error::Error>> {
    let merged_data = generate_merged_data(&conn, start_terminal_id, end_terminal_id)?;
    let transitions = generate_transitions(&merged_data);

    // 将对数据库的查询放入一个作用域，使 stmt 的借用结束
    let terminals: Vec<TerminalRecord> = {
        let mut stmt = conn.prepare(
            "SELECT Proc, ICAO, Name, Rwy
             FROM Terminals
             WHERE ID BETWEEN ?1 AND ?2",
        ).expect("Failed to prepare statement");
        let terminal_iter = stmt
            .query_map(params![start_terminal_id, end_terminal_id], |row| {
                Ok(TerminalRecord {
                    proc: row.get(0)?,
                    icao: row.get(1)?,
                    name: row.get(2)?,
                    rwy: row.get(3)?,
                })
            })
            .expect("Query execution failed");
        terminal_iter
            .map(|res| res.expect("Failed to get row"))
            .collect()
    };

    // 关闭数据库连接（此时 stmt 已经离开作用域，不再借用 conn）
    drop(conn);

    // 过滤掉 ICAO 中包含数字的记录
    let re = Regex::new(r"\d").unwrap();
    let mut terminals: Vec<TerminalRecord> = terminals
        .into_iter()
        .filter(|rec| !re.is_match(&rec.icao))
        .collect();

    // 合并 transitions 与查询到的 terminals
    terminals.extend(transitions);

    // 确保输出目录存在
    fs::create_dir_all(&format!("{}Supplemental\\SID", navdata_path)).ok();
    fs::create_dir_all(&format!("{}Supplemental\\STAR", navdata_path)).ok();

    // ------------------ 处理 Rwy 字段为空的情况 ------------------
    let (others, to_process): (Vec<_>, Vec<_>) =
        terminals.into_iter().partition(|rec| rec.rwy.is_some());

    let mut processed_rows = Vec::new();
    for rec in to_process {
        // 在 merged_data 中查找 ICAO 与 Name 匹配的记录
        let matching: Vec<&MergedRecord> = merged_data
            .iter()
            .filter(|m| m.icao == rec.icao && m.terminal == rec.name)
            .collect();
        // 提取唯一的 Rwy 值
        let mut unique_rw: Vec<String> = matching
            .iter()
            .filter_map(|m| m.rwy.clone())
            .collect();
        unique_rw.sort();
        unique_rw.dedup();

        if unique_rw.is_empty() {
            processed_rows.push(rec);
        } else {
            for rwy in unique_rw {
                let mut new_rec = rec.clone();
                new_rec.rwy = Some(rwy);
                processed_rows.push(new_rec);
            }
        }
    }

    let mut combined = Vec::new();
    combined.extend(others);
    combined.extend(processed_rows);

    Ok((combined, merged_data))
}

/// 增强版文件解析函数，直接返回排序后的条目
fn parse_and_sort_procedures(filename: &Path) -> (Vec<(String, u32)>, u32) {
    let mut entries = Vec::new();
    let mut max_seq = 0;

    if let Ok(file) = fs::File::open(filename) {
        let re = Regex::new(r"^Procedure\.(\d+)=([^.]+)\.(\S+)").unwrap();
        
        for line in BufReader::new(file).lines().filter_map(Result::ok) {
            if let Some(caps) = re.captures(&line) {
                if let (Some(num_str), Some(name), Some(rwy)) = 
                    (caps.get(1), caps.get(2), caps.get(3)) 
                {
                    if let Ok(num) = num_str.as_str().parse::<u32>() {
                        let key = format!("{}.{}", name.as_str(), rwy.as_str());
                        entries.push((key, num));
                        max_seq = max_seq.max(num);
                    }
                }
            }
        }
    }

    // 按序号升序排序（核心修改）
    entries.sort_by_key(|(_, num)| *num);
    
    (entries, max_seq + 1)
}

/// 左侧补零函数，若长度不足 width，则在前面补 '0'
fn zfill(s: &str, width: usize) -> String {
    if s.len() >= width {
        s.to_string()
    } else {
        let pad = "0".repeat(width - s.len());
        format!("{}{}", pad, s)
    }
}

fn write_to_file(icao: &str, proc: &str, data: &[TerminalRecord], navdata_path: &str) {
    let filename = match proc {
        "2" => format!("{}/Supplemental/SID/{}.sid", navdata_path, icao),
        "1" => format!("{}/Supplemental/STAR/{}.star", navdata_path, icao),
        "3" => format!("{}/Supplemental/STAR/{}.app", navdata_path, icao),
        "6" => format!("{}/Supplemental/SID/{}.sidtrs", navdata_path, icao),
        "A" => format!("{}/Supplemental/STAR/{}.apptrs", navdata_path, icao),
        _ => return,
    };

    // 创建目录（跨平台路径处理）
    if let Some(parent) = Path::new(&filename).parent() {
        let _ = fs::create_dir_all(parent);
    }

    // 解析现有文件内容
    let (existing_entries, seqn) = parse_and_sort_procedures(Path::new(&filename));
    // 将现有条目转为 HashMap
    let mut proc_dict: HashMap<_, _> = existing_entries.into_iter().collect();
    // 初始化 next_seq
    let mut next_seq = seqn;

    // 处理新增条目
    let new_entries = Vec::new();
    for rec in data {
        if let Some(rwy_val) = &rec.rwy {
            // 使用 zfill 统一处理补零
            let padded_rwy = zfill(rwy_val, 2); // 强制补到2位
            let name_rwy = format!("{}.{}", rec.name, padded_rwy);
            
            if !proc_dict.contains_key(&name_rwy) {
                proc_dict.insert(name_rwy, next_seq);
                next_seq += 1;
            }
        }
    }

    // 合并新旧条目（保留原有序号）
    proc_dict.extend(new_entries.into_iter());

    // 按序号升序排序（核心修改部分）
    let mut sorted_entries: Vec<(&String, &u32)> = proc_dict.iter().collect();
    sorted_entries.sort_by(|a, b| a.1.cmp(b.1));

    // 构建排序后的内容
    let mut sorted_content = Vec::new();
    for (name_rwy, &num) in sorted_entries {
        if let Some((proc_name, rwy)) = name_rwy.split_once('.') {
            sorted_content.push(format!("Procedure.{}={}.{}", num, proc_name, rwy));
        }
    }

    // 读取原有文件内容（保留非列表部分）
    let original_content = fs::read_to_string(&filename)
        .unwrap_or_else(|_| String::new())
        .lines()
        // 跳过 [list] 部分和已处理的行
        .skip_while(|line| line.trim() != "[list]")
        .skip(1)
        .filter(|line| !line.starts_with("Procedure."))
        .map(|s| s.to_string())
        .collect::<Vec<_>>();

    // 构建最终文件内容
    let mut final_content = vec!["[list]".to_string()];
    final_content.extend(sorted_content);
    final_content.extend(original_content);

    // 写入文件（带换行符）
    if let Err(e) = fs::write(&filename, final_content.join("\n")) {
        eprintln!("Failed to write {}: {}", filename, e);
    }
}

/// 主逻辑：获取 Terminals 数据后对每个 ICAO 与 proc 组合并行调用写文件函数
pub fn list_generate(
    conn: Connection,
    start_terminal_id: i32,
    end_terminal_id: i32,
    navdata_path: &str,
) -> Result<Vec<MergedRecord>, Box<dyn std::error::Error>> {
    let (terminals, merged_data) = get_terminals(conn, start_terminal_id, end_terminal_id, navdata_path)?;
    let merged_data = merged_data.clone();
    let terminals_arc = Arc::new(terminals);
    let navdata_path = navdata_path.to_string();
    let mut handles = Vec::new();

    let unique_icaos: HashSet<String> = terminals_arc
        .iter()
        .map(|rec| rec.icao.clone())
        .collect();

    for icao in unique_icaos {
        for proc in &["1", "2", "3", "6", "A"] {
            let filtered: Vec<TerminalRecord> = terminals_arc
                .iter()
                .filter(|rec| rec.icao == icao && rec.proc.to_string() == *proc)
                .cloned()
                .collect();
            if !filtered.is_empty() {
                let icao_clone = icao.clone();
                let proc_clone = proc.to_string();
                let data_clone = filtered;
                let navdata_path_clone = navdata_path.clone();
                let handle = thread::spawn(move || {
                    write_to_file(&icao_clone, &proc_clone, &data_clone, &navdata_path_clone);
                });
                handles.push(handle);
            }
        }
    }

    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    Ok(merged_data)
}
