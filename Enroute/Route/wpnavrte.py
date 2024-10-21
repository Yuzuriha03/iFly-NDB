import warnings
warnings.filterwarnings('ignore')
import pandas as pd
import os
from geographiclib.geodesic import Geodesic

def dms_to_decimal_latitude(dms):
    direction = dms[0]
    degrees = int(dms[1:3])
    minutes = int(dms[3:5])
    seconds = int(dms[5:])
    
    decimal = degrees + (minutes / 60) + (seconds / 3600)
    
    if direction == 'S':
        decimal = -decimal
    
    return round(decimal, 8)

def dms_to_decimal_longitude(dms):
    direction = dms[0]
    degrees = int(dms[1:4])
    minutes = int(dms[4:6])
    seconds = int(dms[6:])
    
    decimal = degrees + (minutes / 60) + (seconds / 3600)
    
    if direction == 'W':
        decimal = -decimal
    
    return round(decimal, 8)

def load_coordinate_from_table(conn, ident, table_name):
    data = []
    cursor = conn.cursor()
    query = f"SELECT Latitude, Longtitude FROM {table_name} WHERE Ident = ?"
    cursor.execute(query, (ident,))
    
    for row in cursor.fetchall():
        latitude = row[0]
        longitude = row[1]
        data.append({
            "Latitude": latitude,
            "Longitude": longitude
        })
    return data

def geodesic_distance(lat1, lon1, lat2, lon2):
    geod = Geodesic.WGS84
    g = geod.Inverse(lat1, lon1, lat2, lon2)
    return g['s12'] / 1852

def update_coordinates(conn, ident, latitude, longitude, point_type):
    best_match = None
    min_distance = float('inf')

    if point_type in ['VORDME', 'NDB']:
        navaids = load_coordinate_from_table(conn, ident, "Navaids")
        for coords in navaids:  # Iterate directly over the list
            nav_lat, nav_long = coords['Latitude'], coords['Longitude']
            distance = geodesic_distance(latitude, longitude, nav_lat, nav_long)
            if distance <= 5 and distance < min_distance:
                best_match = (nav_lat, nav_long)
                min_distance = distance

    elif point_type in ['DESIGNATED_POINT']:
        waypoints = load_coordinate_from_table(conn, ident, "Waypoints")
        for coords in waypoints:  # Iterate directly over the list
            wpt_lat, wpt_long = coords['Latitude'], coords['Longitude']
            distance = geodesic_distance(latitude, longitude, wpt_lat, wpt_long)
            if distance <= 5 and distance < min_distance:
                best_match = (wpt_lat, wpt_long)
                min_distance = distance

    if best_match:
        latitude, longitude = best_match

    return latitude, longitude

def wpnavrte(conn, csv_file_path, navdata_path):
    
    if conn:
        csvPD = pd.read_csv(csv_file_path, encoding='gbk')
        
        RID = csvPD['TXT_DESIG'].tolist()  # 航路Ident列表
        SPC = csvPD['CODE_POINT_START'].tolist()  # 起始点名称列表
        STP = csvPD['CODE_TYPE_START'].tolist()  # 起始点类型列表
        SLA = csvPD['GEO_LAT_START_ACCURACY'].tolist()  # 起始点纬度列表
        SLO = csvPD['GEO_LONG_START_ACCURACY'].tolist()  # 起始点经度列表
        EPC = csvPD['CODE_POINT_END'].tolist()  # 结束点名称列表
        ETP = csvPD['CODE_TYPE_END'].tolist()  # 结束点类型列表
        ELA = csvPD['GEO_LAT_END_ACCURACY'].tolist()  # 结束点纬度列表
        ELO = csvPD['GEO_LONG_END_ACCURACY'].tolist()  # 结束点经度列表
                
        airway_segments = []
        
        # 编号从001开始，逐步递增
        segment_number = 1
        previous_airway_ident = ""
        
        for i in range(len(SPC)):
            airway_ident = RID[i]
            start_ident = SPC[i]
            if start_ident == "****":
                start_ident = "72PCA"
            if start_ident == "AIWD50/CH":
                start_ident = "CH050"
    
            start_lat, start_long = update_coordinates(conn, start_ident, float(dms_to_decimal_latitude(SLA[i])), float(dms_to_decimal_longitude(SLO[i])), STP[i])
        
            # 检查是否是同一航路的最后一个点
            if airway_ident != previous_airway_ident and previous_airway_ident != "":
                # 添加上一个航路的最后一个点
                end_ident = EPC[i-1]
                if end_ident == "AIWD50/CH":
                    end_ident = "CH050"
                end_lat, end_long = update_coordinates(conn, end_ident, float(dms_to_decimal_latitude(ELA[i-1])), float(dms_to_decimal_longitude(ELO[i-1])), ETP[i-1])
               
                airway_segments.append(f"{previous_airway_ident} {segment_number:03d} {end_ident} {end_lat:.6f} {end_long:.6f}")
                segment_number = 1  # 重置编号
        
            # 添加起始点
            airway_segments.append(f"{airway_ident} {segment_number:03d} {start_ident} {start_lat:.6f} {start_long:.6f}")
            segment_number += 1
            previous_airway_ident = airway_ident
        
        # 处理最后一个航路的最后一个点
        end_ident = EPC[-1]
        end_lat, end_long = update_coordinates(conn, end_ident, float(dms_to_decimal_latitude(ELA[-1])), float(dms_to_decimal_longitude(ELO[-1])), ETP[-1])
        
        airway_segments.append(f"{previous_airway_ident} {segment_number:03d} {end_ident} {end_lat:.6f} {end_long:.6f}")
        
        # 按照Ident顺序进行排序
        airway_segments.sort(key=lambda segment: segment.split()[0]) 
        
        # 保存结果到文件
        output_folder = f'{navdata_path}/Supplemental'
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)
        
        output_file_path = os.path.join(output_folder, 'wpnavrte.txt')
        with open(output_file_path, 'w', encoding='utf-8') as file:
            for segment in airway_segments:
                file.write(segment + '\n')
        file2 = os.path.abspath(output_file_path)
    
    return file2