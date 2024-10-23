import os
import warnings

warnings.filterwarnings('ignore')

def generate_converted_rows(rows):
    for row in rows:
        Ident, Latitude, Longtitude = row
        if Ident == "AIDW5":
            Ident = "CH050"
        Latitude_str = f"{Latitude:.6f}".rjust(10)
        Longtitude_str = f"{Longtitude:.6f}".rjust(11)
        
        result = f"{Ident:<24}{Ident:<5}{Latitude_str}{Longtitude_str}"
        yield (Latitude, result)

def wpnavfix(conn, navdata_path):
    if conn:
        cursor = conn.cursor()
        cursor.execute("SELECT ID FROM waypoints WHERE Ident = '89E80'") # 查找第二个 Ident = 89E80 的记录
        ids = cursor.fetchall()
        if len(ids) < 2:
            print("没有足够的记录")
            return
        second_id = ids[1][0]  # 获取第二个记录的 ID
        # 查询waypoints表格并批量获取
        cursor.execute("SELECT Ident, Latitude, Longtitude FROM waypoints WHERE ID > ?", (second_id,))
        rows = cursor.fetchall()
        converted_rows = list(generate_converted_rows(rows)) # 使用生成器来生成转换后的行
        converted_rows.sort(key=lambda x: x[0])  # 按照Latitude从小到大排序
        output_folder = os.path.join(navdata_path, "Supplemental") # 保存结果到文件
        os.makedirs(output_folder, exist_ok=True)
        
        output_file_path = os.path.join(output_folder, 'wpnavfix.txt')
        with open(output_file_path, 'w', encoding='utf-8') as file:
            for _, result in converted_rows:
                file.write(result + '\n')
        print(f"wpnavfix已保存到 {output_file_path}")
