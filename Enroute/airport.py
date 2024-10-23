import os
import warnings

warnings.filterwarnings('ignore')

def generate_converted_rows(rows):
    for row in rows:
        ICAO, Latitude, Longtitude = row
        Latitude_str = format(Latitude, '.6f')
        Longtitude_str = format(Longtitude, '.6f')
        
        # 检查并调整Longtitude_str长度
        if len(Longtitude_str) < 11:
            Longtitude_str = Longtitude_str.rjust(11)
        # 检查并调整Latitude_str长度
        if len(Latitude_str) < 10:
            Latitude_str = Latitude_str.rjust(10)
        
        # 格式化结果
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
        if not os.path.exists(output_file_path):
            os.makedirs(output_file_path)
        with open(output_file_path, 'w') as file:
            for _, result in converted_rows:
                file.write(result + '\n')
        print(f"airport.dat已保存到{output_file_path}")
        