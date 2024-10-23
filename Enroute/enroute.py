import os
import time
import warnings
from Enroute.supp import supp
from Enroute.airport import airport
from Enroute.wpnavaid import wpnavaid
from Enroute.wpnavapt import wpnavapt
from Enroute.wpnavfix import wpnavfix
from Enroute.Route.wpnavrte import wpnavrte
from Enroute.Route.check_route import check_route
from Enroute.Route.order_route import order_route
from Enroute.Route.insert_route import insert_route

warnings.filterwarnings('ignore')

def enroute(conn, route_file, navdata_path, csv):
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

        airport(conn, start_airport_id, navdata_path)
        supp(conn, start_airport_id, navdata_path)
        input_time = wpnavapt(conn, start_airport_id, navdata_path)
        wpnavaid(conn, navdata_path)
        wpnavfix(conn, navdata_path)
        file2 = wpnavrte(conn, csv, navdata_path)
        check_route(route_file, file2)
        insert_route(route_file, file2)
        order_route(route_file)
        
        end_time = time.time()
        run_time = end_time - start_time - input_time
        print(f"Enroute数据转换完毕，用时：{round(run_time,3)}秒")
