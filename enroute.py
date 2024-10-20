import warnings
warnings.filterwarnings('ignore')
import os
import time
from airport import airport
from supp import supp
from wpnavapt import wpnavapt
from wpnavaid import wpnavaid
from wpnavfix import wpnavfix
from wpnavrte import wpnavrte
from check_route import check_route
from insert_route import inser_route
from order_route import order_route

def get_file_path(prompt, file_extension):

    while True:
        file_path = input(prompt).strip().strip('\'"')  # 去除首尾空格和引号
        if os.path.exists(file_path) and file_path.endswith(file_extension):
            return file_path
        else:
            print(f"文件路径无效或不是一个{file_extension}文件，请重新输入。")
        
def get_start_ids():
    while True:
        user_input = input("请输入要转换数据的起始AirportID，起始NavaidID和起始WaypointID，用空格分隔三者：")
        ids = user_input.split()
        if len(ids) == 3 and all(id.isdigit() for id in ids):
            start_airport_id = int(ids[0])
            start_navaid_id = int(ids[1])
            start_waypoint_id = int(ids[2])
            return start_airport_id, start_navaid_id, start_waypoint_id
        else:
            print("请输入有效的数字，并用空格分隔！")

def enroute(conn, file1, csv):
    # 用户指定的开始转换的ID
    start_airport_id, start_navaid_id, start_waypoint_id = get_start_ids()
    start_time = time.time()

    airport(conn, start_airport_id)
    supp(conn, start_airport_id)
    input_time = wpnavapt(conn, start_airport_id)
    wpnavaid(conn, start_navaid_id)
    wpnavfix(conn, start_waypoint_id)
    file2 = wpnavrte(conn, csv)
    check_route(file1, file2)
    inser_route(file1, file2)
    order_route(file1)
    
    end_time = time.time()
    run_time = end_time - start_time - input_time
    print(f"Enroute数据转换完毕，用时：{round(run_time,3)}秒")
