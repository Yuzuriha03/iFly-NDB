import os
import re
import time
import logging
import sqlite3
import warnings
from Enroute.enroute import enroute
from Terminals.legs import terminals

warnings.filterwarnings('ignore')

# 设置日志配置
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_file_path(prompt, file_extension):
    while True:
        file_path = input(prompt).strip()  # 去除首尾空格
        # 去掉开头的特殊字符和引号
        file_path = file_path.lstrip("'\"& ").rstrip("'\" ")
        print(file_path)
        if os.path.exists(file_path) and file_path.endswith(file_extension):
            logging.info(f"选择的文件: {file_path}")
            return file_path
        else:
            logging.warning(f"无效的文件路径或不是一个{file_extension}文件。请重新输入。")

def get_db_connection(prompt):
    required_tables = set(["AirportCommunication", "AirportLookup", "Airports",
                           "AirwayLegs", "Airways", "config", "Gls", "GridMora",
                           "Holdings", "ILSes", "Markers", "MarkerTypes", "NavaidLookup",
                           "Navaids", "NavaidTypes", "Runways", "SurfaceTypes", "TerminalLegs",
                           "TerminalLegsEx", "Terminals", "TrmLegTypes", "WaypointLookup", "Waypoints"])
    while True:
        # 用户输入db3文件路径
        db_path = get_file_path(prompt, '.db3')
        conn = True
        # 检查文件路径是否有效
        if not os.path.exists(db_path) or not db_path.endswith('.db3'):
            logging.warning("无效的db3文件。请重新输入db3文件路径。")
            continue
        else:
            conn = sqlite3.connect(db_path)
            logging.info(f"连接到数据库: {db_path}")
                        
            # 读取文件内的表头
            tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()
            tables = set([table[0] for table in tables])
                        
            # 检查是否包含所有必需的表格
            if not required_tables.issubset(tables):
                logging.warning("所读取文件不是Fenix数据库格式。请重新输入db3文件路径。")
                conn.close()  # 关闭错误的数据库连接
                continue
                        
            return conn  # 返回有效的数据库连接对象

def get_route_file():
    paths_to_check = {
        "Microsoft Store版": os.path.expandvars(r'%LocalAppData%\Packages\Microsoft.FlightSimulator_8wekyb3d8bbwe\LocalCache\UserCfg.opt'),
        "Steam版": os.path.expandvars(r'%AppData%\Microsoft Flight Simulator\UserCfg.opt')
    }
    
    route_files = {}

    for version, user_cfg_path in paths_to_check.items():
        if os.path.exists(user_cfg_path):
            with open(user_cfg_path, 'r') as file:
                for line in file:
                    if 'InstalledPackagesPath' in line:
                        match = re.search(r'"(.*?)"', line)
                        route_file = match.group(1)
                        route_file = os.path.join(route_file, 'Community\\ifly-aircraft-737max8\\Data\\navdata\\Permanent\\WPNAVRTE.txt')
                        route_files[version] = route_file
                        break
    
    if route_files:
        available_files = {version: rf for version, rf in route_files.items() if os.path.exists(rf)}
        
        if len(available_files) > 1:
            print("找到多个航路文件目录:")
            for i, (version, rf) in enumerate(available_files.items(), 1):
                print(f"{i}: {version} 目录 - {rf}")
            choice = int(input("请选择使用的路径 (输入数字): ")) - 1
            route_file = list(available_files.values())[choice]
            navdata_path = os.path.dirname(os.path.dirname(route_file))
        else:
            route_file = list(available_files.values())[0]
            navdata_path = os.path.dirname(os.path.dirname(route_file))
        
        logging.info(f"程序已自动找到iFly航路文件目录: {route_file}")
        return route_file, navdata_path
    else:
        logging.warning("无法找到iFly航路文件目录，请手动指定路径：")
        route_file = get_file_path("(位于Community\\ifly-aircraft-737max8\\Data\\navdata\\Permanent\\WPNAVRTE.txt)：", "WPNAVRTE.txt")
        navdata_path = os.path.dirname(os.path.dirname(route_file))
        return route_file, navdata_path

def get_terminal_ids():
    while True:
        user_input = input("请输入要转换终端程序集的起始TerminalID和结束TerminalID，用空格分隔二者：")
        terminal_ids = user_input.split()
        if len(terminal_ids) == 1 and terminal_ids[0].isdigit():
            start_terminal_id = int(terminal_ids[0])
            print("终止ID未输入，将自动转换到数据库中最后一个终端程序")
            end_terminal_id = 999999999  # 设定终止ID为999999999
            return start_terminal_id, end_terminal_id
        elif len(terminal_ids) == 2 and all(id.isdigit() for id in terminal_ids):
            start_terminal_id = int(terminal_ids[0])
            end_terminal_id = int(terminal_ids[1])
            return start_terminal_id, end_terminal_id
        else:
            print("请输入有效的数字，并用空格分隔！")

def countdown_timer(seconds):
    while seconds:
        print(f"处理结束，程序将在 {seconds} 秒钟后关闭", end='', flush=True)
        time.sleep(1)
        seconds -= 1
        print('\r', end='', flush=True)
    os._exit(0)  # 强制退出程序

if __name__ == "__main__":
    # 连接到数据库
    conn = get_db_connection("请输入Fenix的nd.db3文件路径：")
    csv = get_file_path("请输入NAIP RTE_SEG.csv文件路径：", "RTE_SEG.csv")
    route_file, navdata_path = get_route_file()
    # 获取起止 TerminalID
    start_terminal_id, end_terminal_id = get_terminal_ids()
    logging.info("开始处理Enroute部分")
    enroute(conn, route_file, navdata_path, csv)
    logging.info("开始处理Terminals部分")
    terminals(conn, navdata_path, start_terminal_id, end_terminal_id)
    countdown_timer(10)
