import os
import re
import sys
import time
import shutil
import logging
import sqlite3
import warnings
import subprocess
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
        "MSFS2020 (Microsoft Store)": os.path.expandvars(r'%LocalAppData%\Packages\Microsoft.FlightSimulator_8wekyb3d8bbwe\LocalCache\UserCfg.opt'),
        "MSFS2020 (Steam)": os.path.expandvars(r'%AppData%\Roaming\Microsoft Flight Simulator\UserCfg.opt'),
        "MSFS2024 (Microsoft Store)": os.path.expandvars(r'%LocalAppData%\Packages\Microsoft.Limitless_8wekyb3d8bbwe\LocalCache\UserCfg.opt'),
        "MSFS2024 (Steam)": os.path.expandvars(r'%AppData%\Roaming\Microsoft Flight Simulator 2024\UserCfg.opt')
    }
    
    route_files = {}

    for version, user_cfg_path in paths_to_check.items():
        if os.path.exists(user_cfg_path):
            with open(user_cfg_path, 'r') as file:
                for line in file:
                    if 'InstalledPackagesPath' in line:
                        match = re.search(r'"(.*?)"', line)
                        if match:
                            route_file = match.group(1)
                            route_file = os.path.join(
                                route_file,
                                'Community\\ifly-aircraft-737max8\\Data\\navdata\\Permanent\\WPNAVRTE.txt',
                            )
                            route_files[version] = route_file
                        break
    
    if route_files:
        available_files = {version: rf for version, rf in route_files.items() if os.path.exists(rf)}
        
        if len(available_files) > 1:
            print("找到多个航路文件目录，将自动处理所有目录:")
            for i, (version, rf) in enumerate(available_files.items(), 1):
                print(f"{i}: {version} 目录 - {rf}")
            
            all_files = list(available_files.values())
            
            # 自动选择所有
            route_file = all_files[0]
            navdata_path = os.path.dirname(os.path.dirname(route_file))
            other_paths = []
            for rf in all_files[1:]:
                other_paths.append(os.path.dirname(os.path.dirname(rf)))
        else:
            route_file = list(available_files.values())[0]
            navdata_path = os.path.dirname(os.path.dirname(route_file))
            other_paths = []
        return route_file, navdata_path, other_paths
    else:
        logging.warning("无法找到iFly航路文件目录，请手动指定路径：")
        route_file = get_file_path("(位于Community\\ifly-aircraft-737max8\\Data\\navdata\\Permanent\\WPNAVRTE.txt)：", "WPNAVRTE.txt")
        navdata_path = os.path.dirname(os.path.dirname(route_file))
        return route_file, navdata_path, []

def get_terminal_ids():
    while True:
        user_input = input("请输入要转换终端程序集的起始TerminalID和结束TerminalID，用空格分隔二者：")
        terminal_ids = user_input.split()
        if len(terminal_ids) == 1 and terminal_ids[0].isdigit():
            start_terminal_id = int(terminal_ids[0])
            print("终止ID未输入，将自动转换到数据库中最后一个终端程序")
            end_terminal_id = 99999999  # 设定终止ID为99999999
            return start_terminal_id, end_terminal_id
        elif len(terminal_ids) == 2 and all(id.isdigit() for id in terminal_ids):
            start_terminal_id = int(terminal_ids[0])
            end_terminal_id = int(terminal_ids[1])
            return start_terminal_id, end_terminal_id
        else:
            print("请输入有效的数字，并用空格分隔！")

def delete_data_navdatasupplemental(navdata_path):
    # 从 navdata_path 返回上级目录
    parent_dir = os.path.dirname(navdata_path)
    target_folder = os.path.join(parent_dir, "navdataSupplemental")
    
    try:
        if os.path.exists(target_folder):
            shutil.rmtree(target_folder, ignore_errors=True)
    except Exception:
        pass

def countdown_timer(seconds):
    while seconds:
        print(f"处理结束，程序将在 {seconds} 秒钟后关闭", end='', flush=True)
        time.sleep(1)
        seconds -= 1
        print('\r', end='', flush=True)
    os._exit(0)  # 强制退出程序

def update_layout_json(navdata_path):
    # navdata_path 类似于 .../Community/ifly-aircraft-737max8/Data/navdata
    # 我们需要找到 .../Community/ifly-aircraft-737max8/layout.json
    
    # 向上回溯到 ifly-aircraft-737max8 目录
    # navdata_path = .../Data/navdata
    # os.path.dirname(navdata_path) = .../Data
    # os.path.dirname(os.path.dirname(navdata_path)) = .../ifly-aircraft-737max8
    
    package_root = os.path.dirname(os.path.dirname(navdata_path))
    layout_json_path = os.path.join(package_root, "layout.json")
    
    if not os.path.exists(layout_json_path):
        logging.warning(f"未找到 layout.json 文件: {layout_json_path}")
        return

    # 假设 MSFSLayoutGenerator.exe 在当前脚本所在目录，或者在系统路径中
    # 这里假设它在当前脚本同级目录下
    current_dir = os.path.dirname(os.path.abspath(__file__))
    
    # 如果是打包后的 exe 运行，需要从临时目录获取
    if hasattr(sys, 'frozen'):
        # Nuitka 打包后的临时目录
        current_dir = os.path.dirname(sys.executable)
        # 或者尝试 sys._MEIPASS (PyInstaller) 或 Nuitka 的特定属性，但通常 sys.executable 的目录或者解压目录
        # 对于 Nuitka --onefile，文件会被解压到临时目录，但 sys.executable 指向的是外面的 exe
        # 实际上 Nuitka --onefile 会设置 sys.frozen = 1
        # 并且会将数据文件解压到临时目录，可以通过 __file__ 获取（在 Nuitka 中 __file__ 指向解压后的脚本路径）
        current_dir = os.path.dirname(os.path.abspath(__file__))

    generator_exe = os.path.join(current_dir, "MSFSLayoutGenerator.exe")
    
    if not os.path.exists(generator_exe):
        logging.warning(f"未找到 MSFSLayoutGenerator.exe: {generator_exe}")
        # 尝试直接调用命令，也许在环境变量中
        generator_exe = "MSFSLayoutGenerator.exe"

    logging.info(f"正在更新 layout.json: {layout_json_path}")
    try:
        # 将 layout.json 文件拖放到 MSFSLayoutGenerator.exe 上，通常意味着将文件路径作为参数传递
        subprocess.run([generator_exe, layout_json_path], check=True)
        logging.info("layout.json 更新成功")
    except subprocess.CalledProcessError as e:
        logging.error(f"更新 layout.json 失败: {e}")
    except FileNotFoundError:
        logging.error("无法执行 MSFSLayoutGenerator.exe，请确保它存在于程序目录或系统路径中。")

if __name__ == "__main__":
    # 连接到数据库
    conn= get_db_connection("请输入Fenix的nd.db3文件路径：")
    csv = get_file_path("请输入NAIP RTE_SEG.csv文件路径：", "RTE_SEG.csv")
    route_file, navdata_path, other_paths = get_route_file()
    # 获取起止 TerminalID
    start_terminal_id, end_terminal_id = get_terminal_ids()
    logging.info("开始处理Enroute部分")
    enroute(conn, route_file, navdata_path, csv)
    logging.info("开始处理Terminals部分")
    terminals(conn, navdata_path, start_terminal_id, end_terminal_id)
    delete_data_navdatasupplemental(navdata_path)
    update_layout_json(navdata_path)
    
    # 同步到其他目录
    for target_path in other_paths:
        try:
            if os.path.exists(target_path):
                shutil.rmtree(target_path)
            shutil.copytree(navdata_path, target_path)
            update_layout_json(target_path)
        except Exception:
            pass

    countdown_timer(10)
