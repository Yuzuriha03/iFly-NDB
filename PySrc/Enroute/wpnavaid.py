import os
import warnings

warnings.filterwarnings('ignore')

def generate_converted_rows(rows):
    type_dict = {
        1: 'VOR', 2: 'VORD', 4: 'VORD', 3: 'DME', 9: 'DME',
        5: 'NDB', 7: 'NDBD', 8: 'ILSD'
    }
    for row in rows:
        ID, Ident, Type, Name, Freq, Channel, Usage, Latitude, Longtitude, Elevation = row
        Type_str = type_dict.get(Type, '')
        Latitude_str = f"{Latitude:.6f}".rjust(10)
        Longtitude_str = f"{Longtitude:.6f}".rjust(11)
        Name_str = Name.ljust(24)[-24:]

        Frequency = float(hex(Freq)[2:])
        while Frequency >= 1000:
            Frequency /= 10
        Frequency_str = f"{Frequency:.2f}"

        final_letter = Usage[-1] if Usage else ''

        result = f"{Name_str}{Ident:<5}{Type_str:<4}{Latitude_str}{Longtitude_str}{Frequency_str}{final_letter}"
        yield (Latitude, result)

def wpnavaid(conn, navdata_path):
    if conn:
        cursor = conn.cursor()
        cursor.execute("SELECT ID FROM navaids WHERE Name = 'DEXIN YANJI'")
        start_id_row = cursor.fetchone()
        
        if not start_id_row:
            print("未找到对应的导航台。")
            return
        
        start_id = start_id_row[0]
        
        cursor.execute("SELECT ID, Ident, Type, Name, Freq, Channel, Usage, Latitude, Longtitude, Elevation FROM navaids WHERE ID > ?", (start_id,))
        rows = cursor.fetchall()
        # 使用生成器来生成转换后的行
        converted_rows = list(generate_converted_rows(rows))
        # 按照Latitude从小到大排序
        converted_rows.sort(key=lambda x: x[0])
        print("转换成功")
        
        output_folder = os.path.join(navdata_path, "Supplemental")
        os.makedirs(output_folder, exist_ok=True)
        
        output_file_path = os.path.join(output_folder, 'wpnavaid.txt')
        with open(output_file_path, 'w', encoding='utf-8') as file:
            for _, result in converted_rows:
                file.write(result + '\n')
        print(f"wpnavaid.txt已保存到 {output_file_path}")