import os
import warnings

warnings.filterwarnings('ignore')

def generate_converted_rows(rows):
    for row in rows:
        ICAO, Latitude, Longtitude = row
        Longtitude_str = f"{Longtitude:.6f}".rjust(11)
        Latitude_str = f"{Latitude:.6f}".rjust(10)
        result = f"{ICAO}{Latitude_str}{Longtitude_str}"
        yield (Latitude, result)

def airport(conn, start_id, navdata_path):

    if conn:
        cursor = conn.cursor()
        
        # 查询airports表格
        cursor.execute("SELECT ICAO, Latitude, Longtitude FROM airports WHERE ID >= ?", (start_id,))
        rows = cursor.fetchall()
        # 转换数据并存储在列表中
        converted_rows = list(generate_converted_rows(rows))
        # 按照Latitude从小到大排序
        converted_rows.sort(key=lambda x: x[0])
        print("转换成功")
    
        # 保存结果到文件
        output_folder = f"{navdata_path}\\Supplemental"
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)
        output_file_path = os.path.join(output_folder, 'airports.dat')
        with open(output_file_path, 'w') as file:
            for _, result in converted_rows:
                file.write(result + '\n')
        print(f"airport.dat已保存到{output_file_path}")
        