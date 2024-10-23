import os
import time
import datetime
import requests
import warnings
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

warnings.filterwarnings('ignore')

def get_declination(lat, lon, date, key):
    url = "https://www.ngdc.noaa.gov/geomag-web/calculators/calculateDeclination"
    params = {
        'lat1': lat,
        'lon1': lon,
        'key': key,
        'resultFormat': 'json',
        'startYear': date.year,
        'startMonth': date.month,
        'startDay': date.day
    }
    response = requests.get(url, params=params)
    data = response.json()
    return data['result'][0]['declination']

def calculate_declination(entry):
    api_key, current_date, rwy_latitude, rwy_longitude, true_heading = entry
    retries = 3
    while retries:
        try:
            declination = get_declination(rwy_latitude, rwy_longitude, current_date, api_key)
            magnetic_heading = round(true_heading - declination)
            return magnetic_heading
        except Exception as e:
            print(f"磁偏角计算错误: {e}")
            retries -= 1
    return round(true_heading)

def generate_tasks(cursor, airport_rows, api_key, current_date):
    for airport_row in airport_rows:
        airport_id, name, icao, latitude, longitude = airport_row
        cursor.execute(
            "SELECT ID, Ident, TrueHeading, Length, Latitude, Longtitude, Elevation "
            "FROM runways WHERE AirportID = ?", (airport_id,)
        )
        runway_rows = cursor.fetchall()
        for runway_row in runway_rows:
            rwy_id, ident, true_heading, length, rwy_latitude, rwy_longitude, rwy_elevation = runway_row
            rwy_latitude_str = format(rwy_latitude, '.6f').rjust(10)
            rwy_longitude_str = format(rwy_longitude, '.6f').rjust(11)
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
                    Frequency_str = format(Frequency, '.2f')
                else:
                    Frequency_str = "000.00"
            else:
                Frequency_str = "000.00"
            task = (api_key, current_date, rwy_latitude, rwy_longitude, true_heading)
            yield (task, name, icao, ident, length, rwy_latitude_str, rwy_longitude_str, Frequency_str, rwy_elevation)

def generate_result_strings(results):
    for result in results:
        (api_key, current_date, rwy_latitude, rwy_longitude, true_heading), name, icao, ident, length, rwy_latitude_str, rwy_longitude_str, Frequency_str, rwy_elevation, magnetic_heading = result
        result_str = f"{name:<24}{icao}{ident:<3}{round(length):05d}{magnetic_heading:03d}{rwy_latitude_str}{rwy_longitude_str}{Frequency_str}{magnetic_heading:03d}{round(rwy_elevation):05d}"
        yield result_str

def wpnavapt(conn, start_apt_id, navdata_path):
    
    if conn:
        cursor = conn.cursor()
        input_start_time = time.time()
        api_key = input("请输入API密钥（密钥获取地址：https://ngdc.noaa.gov/geomag/CalcSurveyFin.shtml）：")
        input_end_time = time.time()
        input_time = input_start_time - input_end_time
        current_date = datetime.date.today()
        cursor.execute("SELECT ID FROM runways WHERE AirportID = ? LIMIT 1", (start_apt_id,))
        start_rwy_id_row = cursor.fetchone()
        start_rwy_id = start_rwy_id_row[0] if start_rwy_id_row else None
        if start_rwy_id is None:
            print("未找到对应的RunwayID")
            conn.close()
            return
        cursor.execute(
            "SELECT ID, Name, ICAO, Latitude, Longtitude FROM airports WHERE ID >= ?", 
            (start_apt_id,)
        )
        airport_rows = cursor.fetchall()
        failed_runways = []
        total_airports = len(airport_rows)
        # 生成tasks
        tasks = list(generate_tasks(cursor, airport_rows, api_key, current_date))
            
        results = []
        with tqdm(total=total_airports, desc="磁偏角计算进度", unit="个") as pbar:
            with ThreadPoolExecutor(max_workers=50) as executor:
                future_to_task = {executor.submit(calculate_declination, task[0]): task for task in tasks}
                for future in as_completed(future_to_task):
                    task = future_to_task[future]
                    try:
                        magnetic_heading = future.result()
                        results.append(task + (magnetic_heading,))
                        pbar.update(1)
                    except Exception as e:
                        print(f"磁偏角计算错误: {e}")
                        results.append(task + (task[0][4],))
    
        
        # 对结果按照 ICAO Rwy排序
        results.sort(key=lambda x: (x[2], x[3]))
        converted_rows = list(generate_result_strings(results))
        
        output_folder = f"{navdata_path}\\Supplemental"
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)
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