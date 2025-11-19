import re
import math
import warnings
from typing import cast

import pandas as pd
from geographiclib.geodesic import Geodesic

warnings.filterwarnings('ignore')

GEODESIC_MODEL: Geodesic = cast(
    Geodesic,
    getattr(Geodesic, "WGS84", Geodesic(a=6378137, f=1 / 298.257223563)),
)

def generate_merged_data(conn, start_terminal_id, end_terminal_id):

    # 查询符合条件的 Airports
    airport_query = """
    SELECT ID, ICAO FROM Airports 
    WHERE ICAO IN ('VQPR', 'OPGT') 
    OR SUBSTR(ICAO, 1, 2) IN ('ZB', 'ZG', 'ZH', 'ZJ', 'ZL', 'ZP', 'ZS', 'ZU', 'ZW', 'ZY')
    """
    airports = pd.read_sql_query(airport_query, conn)
    if airports.empty:
        return pd.DataFrame()
    airport_ids = airports['ID'].tolist()
    
    # 查询符合条件的 Runways
    runways = pd.read_sql_query(f"""
    SELECT ID, AirportID, Ident, TrueHeading, Latitude, Longtitude, Elevation 
    FROM Runways 
    WHERE AirportID IN ({', '.join(map(str, airport_ids))})
    """, conn)
    
    # 查询符合条件的 Terminals
    terminals = pd.read_sql_query(f"""
    SELECT ID, AirportID, Proc, ICAO, Name, Rwy 
    FROM Terminals 
    WHERE ID BETWEEN ? AND ? AND AirportID IN ({', '.join(map(str, airport_ids))})
    """, conn, params=(start_terminal_id, end_terminal_id))
    if terminals.empty:
        return pd.DataFrame()
    
    # 查询符合条件的 TerminalLegs
    terminal_ids_list = terminals['ID'].tolist()
    if not terminal_ids_list:
        return pd.DataFrame()
    terminal_ids = ', '.join(map(str, terminal_ids_list))
    terminal_legs_query = f"""
    SELECT ID, TerminalID, Type, Transition, TrackCode, WptID, WptLat, WptLon, TurnDir, NavID, NavBear, NavDist, Course, Distance, Alt, Vnav, CenterID 
    FROM TerminalLegs 
    WHERE TerminalID IN ({terminal_ids})
    """
    terminal_legs = pd.read_sql_query(terminal_legs_query, conn)
    
    # 查询符合条件的 TerminalLegsEx
    terminal_legs_ex = pd.read_sql_query(f"""
    SELECT ID, IsFlyOver, SpeedLimit, SpeedLimitDescription 
    FROM TerminalLegsEx 
    WHERE ID IN ({', '.join(map(str, terminal_legs['ID'].tolist()))})
    """, conn)
    
    # 合并两个 DataFrame
    merged_data = terminal_legs.merge(terminal_legs_ex, on='ID', how='left')
    
    # 过滤掉 NavID 为空的值
    terminal_legs_nav = terminal_legs.dropna(subset=['NavID'])
    
    # 查询符合条件的 Waypoints 和 Navaids
    waypoints = pd.read_sql_query("SELECT ID, Ident, Latitude, Longtitude FROM Waypoints", conn)
    navaids = pd.read_sql_query(f"""
    SELECT ID, Ident, Latitude, Longtitude 
    FROM Navaids 
    WHERE ID IN ({', '.join(map(str, terminal_legs_nav['NavID'].tolist()))})
    """, conn)
    
    # 处理 SpeedLimit
    merged_data['SpeedLimit'] = merged_data['SpeedLimit'].apply(lambda x: str(int(x)) if pd.notnull(x) else x)
    merged_data['Speed'] = merged_data['SpeedLimit'].fillna('') + merged_data['SpeedLimitDescription'].fillna('')
    
    # 在 merged_data 中加入 ICAO 和 Rwy 列
    merged_data = merged_data.merge(terminals[['ID', 'AirportID', 'Rwy', 'Name']], left_on='TerminalID', right_on='ID', how='left')
    merged_data = merged_data.merge(airports[['ID', 'ICAO']], left_on='AirportID', right_on='ID', how='left', suffixes=('', '_airports'))
    merged_data['Terminal'] = merged_data['Name']
    
    # 加入 Waypoint 的数据
    merged_data = merged_data.merge(waypoints[['ID', 'Ident', 'Latitude', 'Longtitude']], left_on='WptID', right_on='ID', how='left', suffixes=('', '_waypoints'))
    merged_data['Latitude'] = merged_data['WptLat']
    merged_data['Longitude'] = merged_data['WptLon']
    merged_data['Name'] = merged_data['Ident']
    
    # 依据 NavID 加入 Navaids 数据
    merged_data = merged_data.merge(navaids[['ID', 'Ident']], left_on='NavID', right_on='ID', how='left', suffixes=('', '_navaids'))
    merged_data['Frequency'] = merged_data['Ident_navaids']
    
    # 依据 CenterID 加入 Center 数据
    merged_data = merged_data.merge(waypoints[['ID', 'Latitude', 'Longtitude']], left_on='CenterID', right_on='ID', how='left', suffixes=('', '_center'))
    merged_data['CenterLat'] = merged_data['Latitude_center']
    merged_data['CenterLon'] = merged_data['Longtitude_center']
    
    # 重命名列
    merged_data.rename(columns={
        'TurnDir': 'TurnDirection',
        'TrackCode': 'Leg',
        'Distance': 'Dist',
        'Course': 'Heading',
        'Alt': 'Altitude',
        'IsFlyOver': 'CrossThisPoint',
        'Vnav': 'Slope',
    }, inplace=True)

    merged_data['MAP'] = None
    final_columns = [
        'ICAO', 'Rwy', 'Terminal', 'Type', 'Transition', 'Leg', 
        'TurnDirection', 'Name', 'Latitude', 'Longitude', 
        'Frequency', 'NavBear', 'NavDist', 'Heading', 'Dist', 
        'CrossThisPoint', 'Altitude', 'MAP', 'Slope', 'Speed', 
        'CenterLat', 'CenterLon'
    ]
    merged_data = merged_data[final_columns].reset_index(drop=True)

    merged_data['CrossThisPoint'] = merged_data['CrossThisPoint'].astype(str)
    merged_data.loc[merged_data['CrossThisPoint'] == '0', 'CrossThisPoint'] = None

    def normalize_runway_value(raw_value):
        if pd.isnull(raw_value):
            return None
        try:
            return f"{int(float(raw_value)):02d}"
        except (TypeError, ValueError):
            digits = ''.join(filter(str.isdigit, str(raw_value)))
            return digits.zfill(2) if digits else str(raw_value)

    def build_runway_ident(terminal_value):
        if isinstance(terminal_value, str) and len(terminal_value) >= 4:
            ident = terminal_value[1:4].replace('-', '')
            if ident:
                return f"RW{ident}"
        return "RWXX"

    def to_float(value):
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    map_indices = merged_data.index[merged_data['Altitude'] == 'MAP'].tolist()
    for idx in map_indices:
        row_index = int(idx)
        row = merged_data.loc[row_index]
        merged_data.at[row_index, 'MAP'] = 1

        wpt_lat_raw = row['Latitude']
        wpt_lon_raw = row['Longitude']
        wpt_lat = to_float(wpt_lat_raw)
        wpt_lon = to_float(wpt_lon_raw)
        icao_val = row['ICAO']
        rwy_val = normalize_runway_value(row['Rwy'])

        terminal_matches = terminals[terminals['ICAO'] == icao_val]
        if terminal_matches.empty or rwy_val is None:
            continue

        airport_id = int(terminal_matches['AirportID'].iloc[0])
        runway_row = runways[(runways['AirportID'] == airport_id) & (runways['Ident'] == rwy_val)]

        has_waypoint_coords = wpt_lat is not None and wpt_lon is not None
        if has_waypoint_coords:
            waypoint = waypoints[(waypoints['Latitude'] == wpt_lat) & (waypoints['Longtitude'] == wpt_lon)]
            if not waypoint.empty:
                merged_data.at[row_index, 'Name'] = waypoint.iloc[0]['Ident']
            elif not runway_row.empty:
                merged_data.at[row_index, 'Latitude'] = runway_row.iloc[0]['Latitude']
                merged_data.at[row_index, 'Longitude'] = runway_row.iloc[0]['Longtitude']
                merged_data.at[row_index, 'Name'] = build_runway_ident(row['Terminal'])
        elif not runway_row.empty:
            merged_data.at[row_index, 'Latitude'] = runway_row.iloc[0]['Latitude']
            merged_data.at[row_index, 'Longitude'] = runway_row.iloc[0]['Longtitude']
            merged_data.at[row_index, 'Name'] = build_runway_ident(row['Terminal'])

        runway_elevation = to_float(runway_row.iloc[0]['Elevation']) if not runway_row.empty else None
        slope_value = to_float(row['Slope'])
        if runway_elevation is None or slope_value is None:
            continue

        previous_idx = row_index - 1
        previous_altitude_str = None
        while previous_idx >= 0:
            candidate_altitude = merged_data.at[previous_idx, 'Altitude']
            if candidate_altitude:
                previous_altitude_str = candidate_altitude
                break
            previous_idx -= 1

        if previous_altitude_str is None:
            continue

        previous_digits = ''.join(re.findall(r'\d+', str(previous_altitude_str)))
        if not previous_digits:
            continue
        previous_altitude = float(previous_digits)
        previous_latitude = to_float(merged_data.at[previous_idx, 'Latitude'])
        previous_longitude = to_float(merged_data.at[previous_idx, 'Longitude'])
        if previous_latitude is None or previous_longitude is None:
            continue

        current_lat = to_float(row['Latitude'])
        current_lon = to_float(row['Longitude'])
        if current_lat is None or current_lon is None:
            continue

        result = GEODESIC_MODEL.Inverse(
            previous_latitude,
            previous_longitude,
            current_lat,
            current_lon,
        )
        distance_ft = result['s12'] / 0.3048
        altitude = previous_altitude - (distance_ft * math.tan(math.radians(slope_value)))
        fallback_altitude = round(runway_elevation) + 50
        if runway_elevation + 50 <= altitude < 16000:
            merged_data.at[row_index, 'Altitude'] = round(altitude)
        else:
            merged_data.at[row_index, 'Altitude'] = fallback_altitude

    def update_names(df, replacements):
        for old_name, new_name in replacements.items():
            df.loc[df['Name'] == old_name, 'Name'] = new_name

    # 更新特定名称
    replacements = {
        'ZJ400': 'RW15',
        'HJ600': 'RW06',
        'QT800': 'RW27',
        'RQ610': 'RW04',
        'SC600': 'RW33',
        'TK800': 'RW33'
    }
    update_names(merged_data, replacements)

    # ------------------ 新增处理 Rwy 字段为空的逻辑 ------------------
    # 情况1: Transition以RW开头
    mask_rwy = merged_data['Rwy'].isnull() & merged_data['Transition'].str.startswith('RW', na=False)
    merged_data.loc[mask_rwy, 'Rwy'] = merged_data.loc[mask_rwy, 'Transition'].str[2:]
    merged_data.loc[mask_rwy, 'Type'] = 5  # 设置Type为5

    # 情况2: Transition为ALL
    mask_all = (merged_data['Transition'] == 'ALL') & merged_data['Rwy'].isnull()
    rows_to_process = merged_data[mask_all].copy()
    new_rows = pd.DataFrame()

    for index, row in rows_to_process.iterrows():
        icao = row['ICAO']
        terminal = row['Terminal']
        
        # 获取同组其他行的Transition唯一值
        same_group = merged_data[
            (merged_data['ICAO'] == icao) &
            (merged_data['Terminal'] == terminal) &
            (merged_data.index != index)
        ]
        transitions = same_group['Transition'].dropna().unique()
        
        # 过滤以RW开头的Transition并提取跑道号
        rwy_values = [t[2:] for t in transitions if str(t).startswith('RW')]
        
        if rwy_values:
            # 复制当前行并填充跑道号
            dupes = pd.DataFrame([row] * len(rwy_values))
            dupes['Rwy'] = rwy_values
            dupes['Type'] = 5  # 设置Type为5
            new_rows = pd.concat([new_rows, dupes], ignore_index=True)

    # 删除原始ALL行并添加新行
    merged_data = merged_data.drop(rows_to_process.index)
    merged_data = pd.concat([merged_data, new_rows], ignore_index=True)
    
    # ------------------ 新增处理逻辑：IF航段Name为空时填充RW** ------------------
    def fill_rwy_name(group):
        # 遍历分组中的每一行
        for idx, row in group.iterrows():
            # 检查条件：Leg为IF且Name为空且Rwy有效
            if (row['Leg'] == 'IF' and 
                pd.isnull(row['Name']) and 
                pd.notnull(row['Rwy'])):
                
                # 转换Rwy为两位数格式
                try:
                    rwy_str = f"{int(float(row['Rwy'])):02d}"
                except:
                    rwy_str = str(row['Rwy']).zfill(2)
                
                # 更新Name字段
                group.at[idx, 'Name'] = f"RW{rwy_str}"
        
        return group
    
    # 按关键字段分组处理
    grouped = merged_data.groupby(
        ['ICAO', 'Terminal', 'Rwy'], 
        group_keys=False
    ).apply(fill_rwy_name)
    merged_data = cast(pd.DataFrame, grouped)
    
    #整理重排序
    merged_data = merged_data.sort_values(by=['ICAO', 'Terminal', 'Rwy'], kind='mergesort')
    # 重置索引
    merged_data.reset_index(drop=True, inplace=True)
    
    return merged_data
