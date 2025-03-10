use rusqlite::{params, Connection, Result};
use regex::Regex;
use geographiclib_rs::{Geodesic, InverseGeodesic};
use std::collections::HashMap;

pub use MergedDataRow as MergedRecord;

// 各表对应的结构体
#[derive(Debug)]
pub struct Airport {
    pub id: i32,
    pub icao: String,
}

#[derive(Debug)]
pub struct Runway {
    pub id: i32,
    pub airport_id: i32,
    pub ident: String,
    pub true_heading: Option<f64>,
    pub latitude: f64,
    pub longtitude: f64,
    pub elevation: f64,
}

#[derive(Debug)]
pub struct Terminal {
    pub id: i32,
    pub airport_id: i32,
    pub proc: Option<String>,
    pub icao: String,
    pub name: String,
    pub rwy: Option<String>,
}

#[derive(Debug)]
pub struct TerminalLeg {
    pub id: i32,
    pub terminal_id: i32,
    pub type_field: Option<i32>,
    pub transition: Option<String>,
    pub track_code: Option<String>,
    pub wpt_id: Option<i32>,
    pub wpt_lat: Option<f64>,
    pub wpt_lon: Option<f64>,
    pub turn_dir: Option<String>,
    pub nav_id: Option<i32>,
    pub nav_bear: Option<f64>,
    pub nav_dist: Option<f64>,
    pub course: Option<f64>,
    pub distance: Option<f64>,
    pub alt: Option<String>,
    pub vnav: Option<f64>,
    pub center_id: Option<i32>,
}

#[derive(Debug)]
pub struct TerminalLegEx {
    pub id: i32,
    pub is_fly_over: Option<bool>,
    pub speed_limit: Option<f64>,
    pub speed_limit_description: Option<String>,
}

#[derive(Debug)]
pub struct Waypoint {
    pub id: i32,
    pub ident: String,
    pub latitude: f64,
    pub longtitude: f64,
}

#[derive(Debug)]
pub struct Navaid {
    pub id: i32,
    pub ident: String,
    pub latitude: f64,
    pub longtitude: f64,
}

/// 最终合并后的数据结构。为后续处理增加了 terminal_id 字段。
#[derive(Debug, Clone)]
pub struct MergedDataRow {
    pub terminal_id: i32,  // 新增字段
    pub icao: String,
    pub rwy: Option<String>,
    pub terminal: String,
    pub type_field: Option<i32>,
    pub transition: Option<String>,
    pub leg: Option<String>,
    pub turn_direction: Option<String>,
    pub name: Option<String>,
    pub latitude: Option<f64>,
    pub longitude: Option<f64>,
    pub frequency: Option<String>,
    pub nav_bear: Option<f64>,
    pub nav_dist: Option<f64>,
    pub heading: Option<f64>,
    pub dist: Option<f64>,
    pub cross_this_point: Option<String>,
    pub altitude: Option<String>,
    pub map: Option<i32>,
    pub slope: Option<f64>,
    pub speed: Option<String>,
    pub center_lat: Option<f64>,
    pub center_lon: Option<f64>,

    // 保存辅助数据，便于后续合并
    pub wpt_id: Option<i32>,
    pub nav_id: Option<i32>,
    pub center_id: Option<i32>,
}

impl MergedDataRow {
    // 如果需要，可增加 getter 方法，例如：
    pub fn get_wpt_id(&self) -> Option<i32> { self.wpt_id }
    pub fn get_nav_id(&self) -> Option<i32> { self.nav_id }
    pub fn get_center_id(&self) -> Option<i32> { self.center_id }
}

/// 生成合并数据，保留原 Python 逻辑所有功能
pub fn generate_merged_data(conn: &Connection, start_terminal_id: i32, end_terminal_id: i32) -> Result<Vec<MergedDataRow>> {
    // 1. 查询 Airports
    let mut stmt = conn.prepare("
        SELECT ID, ICAO FROM Airports 
        WHERE ICAO = 'VQPR' 
          OR (ICAO LIKE 'ZB%' OR ICAO LIKE 'ZG%' OR ICAO LIKE 'ZH%' OR ICAO LIKE 'ZJ%' 
              OR ICAO LIKE 'ZL%' OR ICAO LIKE 'ZP%' OR ICAO LIKE 'ZS%' OR ICAO LIKE 'ZU%' 
              OR ICAO LIKE 'ZW%' OR ICAO LIKE 'ZY%')
    ")?;
    let airports: Vec<Airport> = stmt.query_map([], |row| {
        Ok(Airport { id: row.get(0)?, icao: row.get(1)? })
    })?.filter_map(Result::ok).collect();

    // 2. 查询 Runways
    let airport_ids: Vec<String> = airports.iter().map(|a| a.id.to_string()).collect();
    let runway_sql = format!("
        SELECT ID, AirportID, Ident, TrueHeading, Latitude, Longtitude, Elevation 
        FROM Runways 
        WHERE AirportID IN ({})
    ", airport_ids.join(", "));
    let mut stmt = conn.prepare(&runway_sql)?;
    let runways: Vec<Runway> = stmt.query_map([], |row| {
        Ok(Runway {
            id: row.get(0)?,
            airport_id: row.get(1)?,
            ident: row.get(2)?,
            true_heading: row.get(3)?,
            latitude: row.get(4)?,
            longtitude: row.get(5)?,
            elevation: row.get(6)?,
        })
    })?.filter_map(Result::ok).collect();

    // 3. 查询 Terminals
    let mut stmt = conn.prepare(&format!("
        SELECT ID, AirportID, Proc, ICAO, Name, Rwy 
        FROM Terminals 
        WHERE ID BETWEEN ? AND ? AND AirportID IN ({})
    ", airport_ids.join(", ")))?;
    let terminals: Vec<Terminal> = stmt.query_map(params![start_terminal_id, end_terminal_id], |row| {
        Ok(Terminal {
            id: row.get(0)?,
            airport_id: row.get(1)?,
            proc: row.get(2)?,
            icao: row.get(3)?,
            name: row.get(4)?,
            rwy: row.get(5)?,
        })
    })?.filter_map(Result::ok).collect();

    // 4. 查询 TerminalLegs
    let terminal_ids: Vec<String> = terminals.iter().map(|t| t.id.to_string()).collect();
    let terminal_ids_str = terminal_ids.join(", ");
    let terminal_legs_sql = format!("
        SELECT ID, TerminalID, Type, Transition, TrackCode, WptID, WptLat, WptLon, TurnDir, NavID, NavBear, NavDist, Course, Distance, Alt, Vnav, CenterID 
        FROM TerminalLegs 
        WHERE TerminalID IN ({})
    ", terminal_ids_str);
    let mut stmt = conn.prepare(&terminal_legs_sql)?;
    let terminal_legs: Vec<TerminalLeg> = stmt.query_map([], |row| {
        Ok(TerminalLeg {
            id: row.get(0)?,
            terminal_id: row.get(1)?,
            type_field: row.get(2)?,
            transition: row.get(3)?,
            track_code: row.get(4)?,
            wpt_id: row.get(5)?,
            wpt_lat: row.get(6)?,
            wpt_lon: row.get(7)?,
            turn_dir: row.get(8)?,
            nav_id: row.get(9)?,
            nav_bear: row.get(10)?,
            nav_dist: row.get(11)?,
            course: row.get(12)?,
            distance: row.get(13)?,
            alt: row.get(14)?,
            vnav: row.get(15)?,
            center_id: row.get(16)?,
        })
    })?.filter_map(Result::ok).collect();

    // 5. 查询 TerminalLegsEx
    let leg_ids: Vec<String> = terminal_legs.iter().map(|tl| tl.id.to_string()).collect();
    let leg_ids_str = leg_ids.join(", ");
    let terminal_legs_ex_sql = format!("
        SELECT ID, IsFlyOver, SpeedLimit, SpeedLimitDescription 
        FROM TerminalLegsEx 
        WHERE ID IN ({})
    ", leg_ids_str);
    let mut stmt = conn.prepare(&terminal_legs_ex_sql)?;
    let terminal_legs_ex: Vec<TerminalLegEx> = stmt.query_map([], |row| {
        Ok(TerminalLegEx {
            id: row.get(0)?,
            is_fly_over: row.get(1)?,
            speed_limit: row.get(2)?,
            speed_limit_description: row.get(3)?,
        })
    })?.filter_map(Result::ok).collect();
    let leg_ex_map: HashMap<i32, &TerminalLegEx> = terminal_legs_ex.iter().map(|ex| (ex.id, ex)).collect();

    // 6. 构造初步的 merged_data 行
    let mut merged_data: Vec<MergedDataRow> = Vec::new();
    for leg in &terminal_legs {
        let ex = leg_ex_map.get(&leg.id);
        let speed = match (ex.and_then(|ex| ex.speed_limit.map(|v| v as i32).map(|v| v.to_string())),
                           ex.and_then(|ex| ex.speed_limit_description.clone())) {
            (Some(sl), Some(desc)) => Some(format!("{}{}", sl, desc)),
            (Some(sl), None) => Some(sl),
            (None, Some(desc)) => Some(desc),
            _ => None,
        };
        merged_data.push(MergedDataRow {
            terminal_id: leg.terminal_id,  // 新增字段
            icao: String::new(),      // 后续更新
            rwy: None,                // 后续更新
            terminal: String::new(),  // 后续更新
            type_field: leg.type_field,
            transition: leg.transition.clone(),
            leg: leg.track_code.clone(),
            turn_direction: leg.turn_dir.clone(),
            name: None,
            latitude: leg.wpt_lat,
            longitude: leg.wpt_lon,
            frequency: None,
            nav_bear: leg.nav_bear,
            nav_dist: leg.nav_dist,
            heading: leg.course,
            dist: leg.distance,
            cross_this_point: ex.and_then(|ex| ex.is_fly_over.map(|v| if v { "1".to_string() } else { "0".to_string() })),
            altitude: leg.alt.clone(),
            map: None,
            slope: leg.vnav,
            speed,
            center_lat: None,
            center_lon: None,
            wpt_id: leg.wpt_id,
            nav_id: leg.nav_id,
            center_id: leg.center_id,
        });
    }

    // 7. 筛选出 NavID 不为空的 TerminalLeg（用于查询 Navaids）
    let terminal_legs_nav: Vec<&TerminalLeg> = terminal_legs.iter().filter(|leg| leg.nav_id.is_some()).collect();

    // 8. 查询 Waypoints
    let mut stmt = conn.prepare("SELECT ID, Ident, Latitude, Longtitude FROM Waypoints")?;
    let waypoints: Vec<Waypoint> = stmt.query_map([], |row| {
        Ok(Waypoint {
            id: row.get(0)?,
            ident: row.get(1)?,
            latitude: row.get(2)?,
            longtitude: row.get(3)?,
        })
    })?.filter_map(Result::ok).collect();
    let waypoint_map: HashMap<i32, &Waypoint> = waypoints.iter().map(|w| (w.id, w)).collect();

    // 9. 查询 Navaids
    let nav_ids: Vec<String> = terminal_legs_nav.iter().filter_map(|leg| leg.nav_id.map(|v| v.to_string())).collect();
    let nav_ids_str = nav_ids.join(", ");
    let navaid_sql = format!("SELECT ID, Ident, Latitude, Longtitude FROM Navaids WHERE ID IN ({})", nav_ids_str);
    let mut stmt = conn.prepare(&navaid_sql)?;
    let navaids: Vec<Navaid> = stmt.query_map([], |row| {
        Ok(Navaid {
            id: row.get(0)?,
            ident: row.get(1)?,
            latitude: row.get(2)?,
            longtitude: row.get(3)?,
        })
    })?.filter_map(Result::ok).collect();
    let navaid_map: HashMap<i32, &Navaid> = navaids.iter().map(|n| (n.id, n)).collect();

    // 10. 合并 merged_data 与 Terminals、Airports，填充 Terminal、Rwy 和 ICAO
    let terminal_map: HashMap<i32, &Terminal> = terminals.iter().map(|t| (t.id, t)).collect();
    for row in merged_data.iter_mut() {
        if let Some(term) = terminal_map.get(&row.terminal_id) {
            row.terminal = term.name.clone();
            row.rwy = term.rwy.clone();
            row.icao = term.icao.clone();
            if let Some(airport) = airports.iter().find(|a| a.id == term.airport_id) {
                row.icao = airport.icao.clone();
            }
        }
    }

    // 11. 合并 Waypoints，依据 WptID 更新 Name, Latitude, Longitude
    for row in merged_data.iter_mut() {
        if let Some(wpt_id) = row.get_wpt_id() {
            if let Some(wp) = waypoint_map.get(&wpt_id) {
                row.name = Some(wp.ident.clone());
                row.latitude = Some(wp.latitude);
                row.longitude = Some(wp.longtitude);
            }
        }
    }

    // 12. 合并 Navaids，依据 NavID 更新 Frequency
    for row in merged_data.iter_mut() {
        if let Some(nav_id) = row.get_nav_id() {
            if let Some(nav) = navaid_map.get(&nav_id) {
                row.frequency = Some(nav.ident.clone());
            }
        }
    }

    // 13. 合并 Center 数据，依据 CenterID 更新 CenterLat, CenterLon
    for row in merged_data.iter_mut() {
        if let Some(center_id) = row.get_center_id() {
            if let Some(wp) = waypoint_map.get(&center_id) {
                row.center_lat = Some(wp.latitude);
                row.center_lon = Some(wp.longtitude);
            }
        }
    }

    // 14. 处理 CrossThisPoint：若值为 "0" 则置为 None
    for row in merged_data.iter_mut() {
        if let Some(ref s) = row.cross_this_point {
            if s == "0" {
                row.cross_this_point = None;
            }
        }
    }

// 15. 对 Altitude == "MAP" 的行进行处理
let geod = Geodesic::wgs84();
let _re_digits = Regex::new(r"\d+").unwrap();
for i in 0..merged_data.len() {
    if let Some(ref alt_str) = merged_data[i].altitude {
        if alt_str == "MAP" {
            merged_data[i].map = Some(1);
            let wpt_lat = merged_data[i].latitude;
            let wpt_lon = merged_data[i].longitude;
            let icao_val = merged_data[i].icao.clone();
            let rwy_val = merged_data[i].rwy.clone().unwrap_or_default();
            let rwy_val = format!("{:0>2}", rwy_val);
            let airport_id_opt = terminals.iter().find(|t| t.icao == icao_val).map(|t| t.airport_id);
            let runway_row_opt = airport_id_opt.and_then(|aid| {
                runways.iter().find(|r| r.airport_id == aid && r.ident == rwy_val)
            });
            if let (Some(lat), Some(lon)) = (wpt_lat, wpt_lon) {
                if let Some(wp) = waypoints.iter().find(|w| (w.latitude - lat).abs() < 1e-6 && (w.longtitude - lon).abs() < 1e-6) {
                    merged_data[i].name = Some(wp.ident.clone());
                } else if let Some(runway_row) = runway_row_opt {
                    merged_data[i].latitude = Some(runway_row.latitude);
                    merged_data[i].longitude = Some(runway_row.longtitude);
                    let new_ident: String = merged_data[i].terminal.chars().skip(1).take(3).filter(|c| *c != '-').collect();
                    merged_data[i].name = Some(format!("RW{}", new_ident));
                }
            } else if let Some(runway_row) = runway_row_opt {
                merged_data[i].latitude = Some(runway_row.latitude);
                merged_data[i].longitude = Some(runway_row.longtitude);
                let new_ident: String = merged_data[i].terminal.chars().skip(1).take(3).filter(|c| *c != '-').collect();
                merged_data[i].name = Some(format!("RW{}", new_ident));
            }
            if let Some(slope) = merged_data[i].slope {
                let mut n: usize = 1;
                let mut previous_altitude_str = merged_data[i.saturating_sub(n)].altitude.clone().unwrap_or_default();
                while previous_altitude_str.is_empty() && n <= i {
                    n += 1;
                    previous_altitude_str = merged_data[i.saturating_sub(n)].altitude.clone().unwrap_or_default();
                }
                if !previous_altitude_str.is_empty() {
                    if let Ok(previous_altitude) = previous_altitude_str.parse::<f64>() {
                        if let (Some(prev_lat), Some(prev_lon), Some(cur_lat), Some(cur_lon)) =
                            (merged_data[i.saturating_sub(n)].latitude,
                             merged_data[i.saturating_sub(n)].longitude,
                             merged_data[i].latitude,
                             merged_data[i].longitude)
                        {
                            // 修改结果解构方式
                            let (s12, _azi1, _azi2) = geod.inverse(prev_lat, prev_lon, cur_lat, cur_lon);
                            let distance_ft = s12 * 3.280839895;  // 米转英尺
                            let altitude_calc = previous_altitude - (distance_ft * slope.to_radians().tan());
                            if let Some(runway_row) = runway_row_opt {
                                if runway_row.elevation + 50.0 <= altitude_calc && altitude_calc < 16000.0 {
                                    merged_data[i].altitude = Some(altitude_calc.round().to_string());
                                } else {
                                    merged_data[i].altitude = Some((runway_row.elevation.round() + 50.0).to_string());
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

    // 16. 更新名称：替换特定值
    let replacements: HashMap<&str, &str> = [
        ("ZJ400", "RW15"),
        ("HJ600", "RW06"),
        ("QT800", "RW27"),
        ("RQ610", "RW04"),
        ("SC600", "RW33"),
        ("TK800", "RW33")
    ].iter().cloned().collect();
    for row in merged_data.iter_mut() {
        if let Some(ref name) = row.name {
            if let Some(&new_name) = replacements.get(name.as_str()) {
                row.name = Some(new_name.to_string());
            }
        }
    }

    // 17. 若 Rwy 为空且 Transition 以 "RW" 开头，则更新 Rwy 并设置 Type 为 5
    for row in merged_data.iter_mut() {
        if row.rwy.is_none() {
            if let Some(ref trans) = row.transition {
                if trans.starts_with("RW") {
                    row.rwy = Some(trans[2..].to_string());
                    row.type_field = Some(5);
                }
            }
        }
    }

    // 18. 处理 Transition 为 "ALL" 且 Rwy 为空的行：复制当前行生成新行，并设置 Type 为 5
    let mut new_rows = Vec::new();
    let mut indices_to_drop = Vec::new();
    for (i, row) in merged_data.iter().enumerate() {
        if let Some(ref trans) = row.transition {
            if trans == "ALL" && row.rwy.is_none() {
                let same_group: Vec<&MergedDataRow> = merged_data.iter()
                    .filter(|r| r.icao == row.icao && r.terminal == row.terminal && (*r as *const _) != (row as *const _))
                    .collect();
                let mut rwy_values = Vec::new();
                for other in same_group {
                    if let Some(ref t) = other.transition {
                        if t.starts_with("RW") {
                            rwy_values.push(t[2..].to_string());
                        }
                    }
                }
                if !rwy_values.is_empty() {
                    for rwy_val in rwy_values {
                        let mut new_row = row.clone();
                        new_row.rwy = Some(rwy_val);
                        new_row.type_field = Some(5);
                        new_rows.push(new_row);
                    }
                    indices_to_drop.push(i);
                }
            }
        }
    }
    merged_data = merged_data.into_iter().enumerate()
        .filter(|(i, _)| !indices_to_drop.contains(i))
        .map(|(_, r)| r)
        .collect();
    merged_data.extend(new_rows);

    // 19. 对 (ICAO, Terminal, Rwy) 分组，若 Leg 为 "IF" 且 Name 为空，则填充 Name 为 "RW" + 两位跑道号
    let mut group_map: HashMap<(String, String, String), Vec<usize>> = HashMap::new();
    for (i, row) in merged_data.iter().enumerate() {
        if let Some(ref rwy) = row.rwy {
            group_map.entry((row.icao.clone(), row.terminal.clone(), rwy.clone())).or_default().push(i);
        }
    }
    for (_group, indices) in group_map {
        for i in indices {
            if let Some(ref leg) = merged_data[i].leg {
                if leg == "IF" && merged_data[i].name.is_none() {
                    if let Some(ref rwy) = merged_data[i].rwy {
                        if let Ok(num) = rwy.parse::<i32>() {
                            merged_data[i].name = Some(format!("RW{:02}", num));
                        } else {
                            merged_data[i].name = Some(format!("RW{}", rwy));
                        }
                    }
                }
            }
        }
    }

    // 20. 排序 merged_data（按照 ICAO, Terminal, Rwy 排序）
    merged_data.sort_by(|a, b| {
        a.icao.cmp(&b.icao)
            .then(a.terminal.cmp(&b.terminal))
            .then(a.rwy.cmp(&b.rwy))
    });

    Ok(merged_data)
}
