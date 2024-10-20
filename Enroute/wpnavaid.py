import warnings
warnings.filterwarnings('ignore')
import os

def generate_converted_rows(rows):
    type_dict = {
    1: 'VOR',
    2: 'VORD',
    4: 'VORD',
    3: 'DME',
    9: 'DME',
    5: 'NDB',
    7: 'NDBD',
    8: 'ILSD'
    }
    for row in rows:
        ID, Ident, Type, Name, Freq, Channel, Usage, Latitude, Longtitude, Elevation = row
        Type_str = type_dict.get(Type, '')
        Latitude_str = format(Latitude, '.6f')
        Longtitude_str = format(Longtitude, '.6f')
        Name_str = Name.ljust(24)[-24:]
        
        # 将Freq转换为十进制字符串
        Frequency = float(hex(Freq)[2:])
        
        # 除到整数部分只有三位
        while Frequency >= 1000:
            Frequency /= 10
        
        Frequency_str = format(Frequency, '.2f')
        
        # 确定最后一个字母
        final_letter = Usage[-1] if Usage else ''
        
        # 检查Longtitude_str长度，若小于等于10则在前面加空格以补到10个长度
        if len(Longtitude_str) <= 10:
            Longtitude_str = Longtitude_str.rjust(11)
        # 检查Latitude_str长度，若小于等于9则在前面加空格以补到10个长度
        if len(Latitude_str) <= 9:
            Latitude_str = Latitude_str.rjust(10)
        
        # 格式化结果
        result = f"{Name_str}{Ident:<5}{Type_str:<4}{Latitude_str}{Longtitude_str}{Frequency_str}{final_letter}"
        yield (Latitude, result)

def wpnavaid(conn, start_id):

    if conn:
        cursor = conn.cursor()
        
        # 查询navadis表格
        cursor.execute("SELECT ID, Ident, Type, Name, Freq, Channel, Usage, Latitude, Longtitude, Elevation FROM navaids WHERE ID >= ?", (start_id,))
        rows = cursor.fetchall()
        # 使用生成器来生成转换后的行
        converted_rows = list(generate_converted_rows(rows))
        # 按照Latitude从小到大排序
        converted_rows.sort(key=lambda x: x[0])
        print("转换成功")
    
        # 保存结果到文件   
        if not os.path.exists('output/'):
            os.makedirs('output/')
        with open('output/wpnavaid.txt', 'w') as file:
            for _, result in converted_rows:
                file.write(result + '\n')
    
        print(f"wpnavaid.txt已保存到程序目录/output")