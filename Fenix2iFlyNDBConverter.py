import warnings
warnings.filterwarnings('ignore')
import os
import sqlite3
import time
import logging
from Enroute.enroute import enroute
from Terminals.legs import terminals

# 设置日志配置
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_file_path(prompt, file_extension):
    while True:
        file_path = input(prompt).strip().strip('\'"&')  # 去除首尾空格引号&
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
    file1 = get_file_path("请输入iFly航路文件路径\n(位于Community\\ifly-aircraft-737max8\\Data\\navdata\\Permanent\\WPNAVRTE.txt)：", "WPNAVRTE.txt")
    csv = get_file_path("请输入NAIP RTE_SEG.csv文件路径：", "RTE_SEG.csv")
    
    logging.info("开始处理Enroute部分")
    enroute(conn, file1, csv)
    logging.info("开始处理Terminals部分")
    terminals(conn)
    countdown_timer(10)
