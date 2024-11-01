import re
import math
import warnings
import pandas as pd
from geographiclib.geodesic import Geodesic

warnings.filterwarnings('ignore')

def generate_merged_data(conn, start_terminal_id, end_terminal_id):

    # 查询符合条件的 Airports
    airport_query = """
    SELECT ID, ICAO FROM Airports 
    WHERE ICAO = 'VQPR' OR (ICAO LIKE 'ZB%' OR ICAO LIKE 'ZG%' OR ICAO LIKE 'ZH%' OR ICAO LIKE 'ZJ%' 
    OR ICAO LIKE 'ZL%' OR ICAO LIKE 'ZP%' OR ICAO LIKE 'ZS%' OR ICAO LIKE 'ZU%' OR ICAO LIKE 'ZW%' OR ICAO LIKE 'ZY%')
    """
    airports = pd.read_sql_query(airport_query, conn)
    
    # 查询符合条件的 Runways
    runways = pd.read_sql_query(f"""
    SELECT ID, AirportID, Ident, TrueHeading, Latitude, Longtitude, Elevation 
    FROM Runways 
    WHERE AirportID IN ({', '.join(map(str, airports['ID'].tolist()))})
    """, conn)
    
    # 查询符合条件的 Terminals
    terminals = pd.read_sql_query(f"""
    SELECT ID, AirportID, Proc, ICAO, Name, Rwy 
    FROM Terminals 
    WHERE ID BETWEEN ? AND ? AND AirportID IN ({', '.join(map(str, airports['ID'].tolist()))})
    """, conn, params=(start_terminal_id, end_terminal_id))
    
    # 检查并处理空的 Rwy 字段
    for index, row in terminals.iterrows():
        if pd.isnull(row['Rwy']) or row['Rwy'].strip() == '':
            while True:
                user_input = input(f"{row['ICAO']}.{row['Name']} 对应的Rwy值为空，请手动输入 Rwy 值：").strip()
                if re.match(r'^\d{2}$', user_input) or re.match(r'^\d{2}[CLR]$', user_input, re.IGNORECASE):
                    terminals.at[index, 'Rwy'] = user_input.upper()
                    break
                else:
                    print("输入无效，请重新输入有效的跑道编号。")
    
    # 查询符合条件的 TerminalLegs
    terminal_ids = ', '.join(map(str, terminals['ID'].tolist()))
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
    merged_data = merged_data[final_columns]

    # 确保'CrossThisPoint'列中的值被转换为字符串
    merged_data['CrossThisPoint'] = merged_data['CrossThisPoint'].astype(str)

    for index, row in merged_data[merged_data['CrossThisPoint'] == '0' ].iterrows():
        merged_data.at[index, 'CrossThisPoint'] = None
    
    # 遍历merged_data，对于Altitude=MAP的行进行操作
    for index, row in merged_data[merged_data['Altitude'] == 'MAP'].iterrows():
        # 赋值MAP列
        merged_data.at[index, 'MAP'] = 1
        # 补ID
        wpt_lat = row['Latitude']
        wpt_lon = row['Longitude']
        # 提取对应列的ICAO列和Rwy值
        icao_val = row['ICAO']
        rwy_val = str(row['Rwy']).zfill(2)
        # 用ICAO值去terminals字典中寻找对应的AirportID
        airport_id = terminals.loc[terminals['ICAO'] == icao_val, 'AirportID'].values[0]
        # runways中找到对应的行
        runway_row = runways[(runways['AirportID'] == airport_id) & (runways['Ident'] == rwy_val)]
    
        if pd.notnull(wpt_lat) and pd.notnull(wpt_lon):
            waypoint = waypoints[(waypoints['Latitude'] == wpt_lat) & (waypoints['Longtitude'] == wpt_lon)]
            if not waypoint.empty:
                merged_data.at[index, 'Name'] = waypoint.iloc[0]['Ident']
            else:
                 # 在runways字典中找到对应行，并填入merged_data
                 if not runway_row.empty:
                     merged_data.at[index, 'Latitude'] = runway_row.iloc[0]['Latitude']
                     merged_data.at[index, 'Longitude'] = runway_row.iloc[0]['Longtitude']
                     new_ident = f"RW{row['Terminal'][1:4].replace('-', '')}"
                     merged_data.at[index, 'Name'] = new_ident
        else:
            # 在runways字典中找到对应行，并填入merged_data
            if not runway_row.empty:
                merged_data.at[index, 'Latitude'] = runway_row.iloc[0]['Latitude']
                merged_data.at[index, 'Longitude'] = runway_row.iloc[0]['Longtitude']
                new_ident = f"RW{row['Terminal'][1:4].replace('-', '')}"
                merged_data.at[index, 'Name'] = new_ident
        
        # 计算高度值
        slope = row['Slope']
        if not runway_row.empty:
            runway_elevation = runway_row.iloc[0]['Elevation']
        if pd.notnull(slope):
            n = 1
            previous_altitude_str = merged_data.at[index - n, 'Altitude']
            while not previous_altitude_str and index - n >= 0:
                n += 1
                previous_altitude_str = merged_data.at[index - n, 'Altitude']
            
            if previous_altitude_str:
                previous_altitude = float(''.join(re.findall(r'\d+', previous_altitude_str)))
                previous_latitude = merged_data.at[index-n, 'Latitude']
                previous_longitude = merged_data.at[index-n, 'Longitude']
                if pd.notnull(previous_latitude) and pd.notnull(previous_longitude):
                    distance = Geodesic.WGS84.Inverse(previous_latitude, previous_longitude, row['Latitude'], row['Longitude'])['s12'] / 0.3048  # 转换为英尺
                    altitude = previous_altitude - (distance * math.tan(math.radians(slope)))
                    if runway_elevation + 50 <= altitude < 16000:
                        merged_data.at[index, 'Altitude'] = round(altitude)
                    else:
                        merged_data.at[index, 'Altitude'] = round(runway_elevation) + 50

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

    return merged_data
