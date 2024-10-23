import os
import warnings

warnings.filterwarnings('ignore')

def supp(conn, start_apt_id, navdata_path):
    if conn:
        output_folder = os.path.join(navdata_path, "Supplemental", "Supp")
        os.makedirs(output_folder, exist_ok=True)

        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT ICAO, TransitionAltitude, TransitionLevel, SpeedLimit "
                "FROM airports WHERE ID >= ?", (start_apt_id,))
            airport_rows = cursor.fetchall()

        for airport_row in airport_rows:
            icao, transition_altitude, transition_level, speed_limit = airport_row
            
            file_content = [
                "[Speed_Transition]",
                f"Speed={speed_limit}",
                "Altitude=10000",
                "[Transition_Altitude]",
                f"Altitude={transition_altitude}",
                "[Transition_Level]",
                f"Altitude={transition_level}"
            ]
            
            output_file_path = os.path.join(output_folder, f'{icao}.supp')
            with open(output_file_path, 'w', encoding='utf-8') as file:
                file.write('\n'.join(file_content))
            
            print(f"supp文件已保存到 {output_file_path}")
