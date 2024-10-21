import warnings
warnings.filterwarnings('ignore')
import os
import time
from Enroute.airport import airport
from Enroute.supp import supp
from Enroute.wpnavapt import wpnavapt
from Enroute.wpnavaid import wpnavaid
from Enroute.wpnavfix import wpnavfix
from Enroute.Route.wpnavrte import wpnavrte
from Enroute.Route.check_route import check_route
from Enroute.Route.insert_route import inser_route
from Enroute.Route.order_route import order_route

def get_file_path(prompt, file_extension):

    while True:
        file_path = input(prompt).strip().strip('\'"')  # 去除首尾空格和引号
        if os.path.exists(file_path) and file_path.endswith(file_extension):
            return file_path
        else:
            print(f"文件路径无效或不是一个{file_extension}文件，请重新输入。")

def enroute(conn, file1, csv):
    if conn:
        start_time = time.time()
        cursor = conn.cursor()
        # 查找 ICAO = ZYYJ 的记录
        cursor.execute("SELECT ID FROM airports WHERE ICAO = 'ZYYJ'")
        start_id_row = cursor.fetchone()
        
        if start_id_row:
            start_id = start_id_row[0]  # 提取ZYYJ对应的 AirportID 值
            # 用户指定的开始转换的ID
            start_airport_id = start_id + 1
        else:
            print("未找到对应的机场。")
        
        navdata_path = os.path.dirname(file1)
        navdata_path = os.path.join(navdata_path, '..')
        navdata_path = os.path.abspath(navdata_path)

        airport(conn, start_airport_id, navdata_path)
        supp(conn, start_airport_id, navdata_path)
        input_time = wpnavapt(conn, start_airport_id, navdata_path)
        wpnavaid(conn, navdata_path)
        wpnavfix(conn, navdata_path)
        file2 = wpnavrte(conn, csv, navdata_path)
        check_route(file1, file2)
        inser_route(file1, file2)
        order_route(file1)
        
        end_time = time.time()
        run_time = end_time - start_time - input_time
        print(f"Enroute数据转换完毕，用时：{round(run_time,3)}秒")

        return navdata_path
