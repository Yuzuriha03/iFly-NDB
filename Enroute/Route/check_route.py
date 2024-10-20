import warnings
warnings.filterwarnings('ignore')

import re

def read_file_to_dict(file_name):
    result_dict = {}
    with open(file_name, 'r', encoding='utf-8') as file:
        for line in file:
            if not line.startswith(';'):
                parts = line.split()
                if len(parts) >= 5:
                    Route_Ident = parts[0]
                    Segment_Number = int(parts[1])
                    Fix_Ident = parts[2]
                    Latitude = parts[3]
                    Longitude = parts[4]
                    if Route_Ident not in result_dict:
                        result_dict[Route_Ident] = []
                    result_dict[Route_Ident].append((Segment_Number, Fix_Ident, Latitude, Longitude))
    return result_dict

def process_dicts(dict1, dict2):
    for key in dict1:
        if key in dict2:
            min_segment1 = min(dict1[key], key=lambda x: x[0])[1]
            min_segment2 = min(dict2[key], key=lambda x: x[0])[1]
            if min_segment1 != min_segment2:
                # 只逆序 Fix_Ident 和经纬度 而保留 Segment_Number 的顺序
                original = dict2[key]
                reversed_fixes = [(fix, lat, lon) for _, fix, lat, lon in sorted(original, key=lambda x: x[0])][::-1]
                dict2[key] = [(seg, reversed_fixes[i][0], reversed_fixes[i][1], reversed_fixes[i][2]) for i, (seg, _, _, _) in enumerate(original)]
    return dict2

def save_dict_to_file(original_file, processed_dict):
    lines = []
    
    # 读取文件内容
    with open(original_file, 'r', encoding='utf-8') as file:
        lines = file.readlines()
    
    naip_route_idents = set()
    pattern = re.compile(r'P\d{2,3}')

    # 收集符合条件的Route_Ident
    for line in lines:
        parts = line.split()
        if len(parts) >= 3:
            Route_Ident = parts[0]
            Fix_ident = parts[2]
            if pattern.match(Fix_ident):
                naip_route_idents.add(Route_Ident)
    
    # 写入文件时处理每一行
    with open(original_file, 'w', encoding='utf-8') as file:
        for line in lines:
            parts = line.split()

            if not line.startswith(';'):
                if len(parts) >= 5:
                    Route_Ident = parts[0]
                    if line.startswith('XX'):
                        continue  # 跳过所有以 "XX" 开头的行
                    if line.startswith(('A', 'B', 'G', 'L', 'M', 'R', 'V', 'W')) and Route_Ident not in naip_route_idents:
                        continue
                    Segment_Number = int(parts[1])
                    if Route_Ident in processed_dict:
                        # 找到对应的 Segment_Number
                        for segment in processed_dict[Route_Ident]:
                            if segment[0] == Segment_Number:
                                file.write(f"{Route_Ident} {str(Segment_Number).zfill(3)} {segment[1]} {segment[2]} {segment[3]}\n")
                                break
                        else:
                            file.write(line)
                    else:
                        file.write(line)
            else:
                file.write(line)

def check_route(file1, file2):      
    # Paths to your files
    file1_dict = read_file_to_dict(file1)
    file2_dict = read_file_to_dict(file2)
    
    # Process the dictionaries
    processed_dict2 = process_dicts(file1_dict, file2_dict)
    
    # Save the processed dictionary back to the original file
    save_dict_to_file(file2, processed_dict2)
    