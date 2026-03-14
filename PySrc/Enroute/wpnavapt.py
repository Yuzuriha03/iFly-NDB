import os
import time
import datetime
import warnings
from tqdm import tqdm
from pygeomag import GeoMag
from concurrent.futures import ProcessPoolExecutor, as_completed

warnings.filterwarnings('ignore')

geo_mag = GeoMag(coefficients_file='wmm/WMMHR_2025.COF', high_resolution=True)
# 获取当前年份的小数表示（如2025.3表示2025年4月左右）
current_date = datetime.datetime.now()
year_decimal = current_date.year + ((current_date.month - 1) / 12.0) + (current_date.day / 365.0)

def get_declination(lat, lon):
    """
    计算指定经纬度的磁偏角，使用本地pygeomag库而非API
    
    参数:
    lat (float): 纬度
    lon (float): 经度
    COF_PATH (str): 磁场系数文件路径
    
    返回:
    float: 磁偏角(度)，保留1位小数
    """
    try:
        result = geo_mag.calculate(glat=float(lat), glon=float(lon), alt=0, time=year_decimal)
        
        # 取磁偏角结果(d属性)并四舍五入保留1位小数
        declination = round(result.d, 1)
        return declination
        
    except Exception as e:
        print(f"计算磁偏角时出错: {e}")
        return 0.0
    
def calculate_declination(entry):
    rwy_latitude, rwy_longitude, true_heading = entry[:3]
    retries = 3
    while retries:
        try:
            declination = get_declination(rwy_latitude, rwy_longitude)
            magnetic_heading = round(true_heading - declination)
            return magnetic_heading
        except Exception as e:
            print(f"磁偏角计算错误: {e}")
            retries -= 1
    return round(true_heading)

def generate_tasks(cursor, airport_rows):
    for airport_row in airport_rows:
        airport_id, name, icao = airport_row[:3]
        cursor.execute(
            "SELECT ID, Ident, TrueHeading, Length, Latitude, Longtitude, Elevation "
            "FROM runways WHERE AirportID = ?", (airport_id,)
        )
        runway_rows = cursor.fetchall()
        for runway_row in runway_rows:
            rwy_id, ident, true_heading, length, rwy_latitude, rwy_longitude, rwy_elevation = runway_row
            rwy_latitude_str = f"{rwy_latitude:.6f}".rjust(10)
            rwy_longitude_str = f"{rwy_longitude:.6f}".rjust(11)
            cursor.execute("SELECT ilsID FROM terminals WHERE RwyID = ? AND ilsID IS NOT NULL", (rwy_id,))
            ils = cursor.fetchone()
            if ils:
                ilsID = ils[0]
                cursor.execute("SELECT Freq FROM ILSes WHERE ID = ?", (ilsID,))
                freq_result = cursor.fetchone()
                if freq_result:
                    Freq = freq_result[0]
                    Frequency = float(hex(Freq)[2:])
                    while Frequency >= 1000:
                        Frequency /= 10
                    Frequency_str = f"{Frequency:.2f}"
                else:
                    Frequency_str = "000.00"
            else:
                Frequency_str = "000.00"
            task = (rwy_latitude, rwy_longitude, true_heading)
            yield (task, name, icao, ident, length, rwy_latitude_str, rwy_longitude_str, Frequency_str, rwy_elevation)

def generate_result_strings(results):
    for result in results:
        task, name, icao, ident, length, rwy_latitude_str, rwy_longitude_str, Frequency_str, rwy_elevation, magnetic_heading = result
        result_str = f"{name:<24}{icao}{ident:<3}{round(length):05d}{magnetic_heading:03d}{rwy_latitude_str}{rwy_longitude_str}{Frequency_str}{magnetic_heading:03d}{round(rwy_elevation):05d}"
        yield result_str

def wpnavapt(conn, start_apt_id, navdata_path):
    if conn:
        cursor = conn.cursor()
        input_start_time = time.time()
        input_time = time.time() - input_start_time
        
        cursor.execute("SELECT ID FROM runways WHERE AirportID = ? LIMIT 1", (start_apt_id,))
        start_rwy_id_row = cursor.fetchone()
        start_rwy_id = start_rwy_id_row[0] if start_rwy_id_row else None
        
        if start_rwy_id is None:
            print("未找到对应的RunwayID")
            conn.close()
            return
        
        cursor.execute(
            "SELECT ID, Name, ICAO FROM airports WHERE ID >= ?", 
            (start_apt_id,)
        )
        airport_rows = cursor.fetchall()  # 确保是列表
        
        failed_runways = []
        total_airports = len(airport_rows)
        
        # 生成tasks
        tasks = list(generate_tasks(cursor, airport_rows))
        total_tasks = len(tasks)
        
        if not total_tasks:
            print("未找到需要处理的跑道数据")
            return

        results = []
        with tqdm(total=total_tasks, desc="磁偏角计算进度", unit="条") as pbar:
            with ProcessPoolExecutor(max_workers=50) as executor:
                future_to_task = {executor.submit(calculate_declination, task[0]): task for task in tasks}
                for future in as_completed(future_to_task):
                    task = future_to_task[future]
                    try:
                        magnetic_heading = future.result()
                        results.append(task + (magnetic_heading,))
                        pbar.update(1)
                    except Exception as e:
                        print(f"磁偏角计算错误: {e}")
                        fallback_heading = round(task[0][2])
                        results.append(task + (fallback_heading,))
                        pbar.update(1)
        
        # 对结果按照 ICAO Rwy排序
        results.sort(key=lambda x: (x[2], x[3]))
        converted_rows = list(generate_result_strings(results))
        
        output_folder = os.path.join(navdata_path, "Supplemental")
        os.makedirs(output_folder, exist_ok=True)
        
        output_file_path = os.path.join(output_folder, 'wpnavapt.txt')
        with open(output_file_path, 'w', encoding='utf-8') as file:
            for row in converted_rows:
                file.write(row + '\n')
        print(f"已保存到 {output_file_path}")
        
        if failed_runways:
            print("以下Runways未能成功计算：")
            for runway_id in failed_runways:
                cursor.execute("SELECT ICAO FROM Airports WHERE ID = (SELECT AirportID FROM Runways WHERE ID = ?)", (runway_id,))
                icao = cursor.fetchone()
                cursor.execute("SELECT Ident FROM Runways WHERE ID = ?", (runway_id,))
                ident = cursor.fetchone()
                print(f"{icao[0]}{ident[0]:<3}")
        
        return input_time
