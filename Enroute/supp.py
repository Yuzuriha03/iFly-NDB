import warnings
warnings.filterwarnings('ignore')
import os

def supp(conn, start_apt_id, navdata_path):
    
    if conn:
        cursor = conn.cursor()
        
        # 查询airports表格
        cursor.execute(
            "SELECT ICAO, TransitionAltitude, TransitionLevel, SpeedLimit "
            "FROM airports WHERE ID >= ?", (start_apt_id,))
        airport_rows = cursor.fetchall()
        
        for i, airport_row in enumerate(airport_rows):
            icao, transition_altitude, transition_level, speed_limit = airport_row
            
            # 文件内容
            file_content = f"[Speed_Transition]\nSpeed={speed_limit}\nAltitude=10000\n"
            file_content += f"[Transition_Altitude]\nAltitude={transition_altitude}\n"
            file_content += f"[Transition_Level]\nAltitude={transition_level}\n"
            
            # 保存结果到文件
            output_folder = f"{navdata_path}\\Supplemental\\Supp"
            if not os.path.exists(output_folder):
                os.makedirs(output_folder)
            
            output_file_path = os.path.join(output_folder, f'{icao}.supp')
            with open(output_file_path, 'w', encoding='utf-8') as file:
                file.write(file_content)
        
        print(f"supp文件已保存到{output_folder}")
