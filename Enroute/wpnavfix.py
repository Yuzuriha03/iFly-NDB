import warnings
warnings.filterwarnings('ignore')
import os

def generate_converted_rows(rows):
    for row in rows:
        Ident, Latitude, Longtitude = row
        if Ident == "AIDW5":
            Ident = "CH050"
        Latitude_str = format(Latitude, '.6f')
        Longtitude_str = format(Longtitude, '.6f')
        
        # 检查并调整Longtitude_str长度
        if len(Longtitude_str) <= 10:
            Longtitude_str = Longtitude_str.rjust(11)
        # 检查并调整Latitude_str长度
        if len(Latitude_str) <= 9:
            Latitude_str = Latitude_str.rjust(10)
        
        # 格式化结果
        result = f"{Ident:<24}{Ident:<5}{Latitude_str}{Longtitude_str}"
        yield (Latitude, result)

def wpnavfix(conn, start_id):
    if conn:
        cursor = conn.cursor()
        
        # 查询waypoints表格
        cursor.execute("SELECT Ident, Latitude, Longtitude FROM waypoints WHERE ID >= ?", (start_id,))
        rows = cursor.fetchall()
        
        # 使用生成器来生成转换后的行
        converted_rows = list(generate_converted_rows(rows))
        
        # 按照Latitude从小到大排序
        converted_rows.sort(key=lambda x: x[0])
        
        # 保存结果到文件
        output_folder = 'output'
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)
        
        output_file_path = os.path.join(output_folder, 'wpnavfix.txt')
        with open(output_file_path, 'w', encoding='utf-8') as file:
            for _, result in converted_rows:
                file.write(result + '\n')
        
        print(f"wpnavfix已保存到程序目录/output")