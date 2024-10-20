import warnings
warnings.filterwarnings('ignore')
import os

def read_file_to_dict(file_path):
    data_dict = {}
    with open(file_path, 'r', encoding='utf-8') as file:
        for line in file:
            if not line.startswith(';'):
                parts = line.strip().split()
                route_ident = parts[0]
                segment_number = int(parts[1])
                fix_ident = parts[2]
                latitude = parts[3]
                longitude = parts[4]
                if route_ident not in data_dict:
                    data_dict[route_ident] = []
                data_dict[route_ident].append({
                    'Segment_Number': segment_number,
                    'Fix_Ident': fix_ident,
                    'Latitude': latitude,
                    'Longitude': longitude
                })
    return data_dict

def renumber_segments(data_dict):
    for route_ident, segments in data_dict.items():
        for i, segment in enumerate(segments, start=1):
            segment['Segment_Number'] = f'{i:03}'
    return data_dict

def write_dict_to_file(data_dict, file_path):
    with open(file_path, 'w', encoding='utf-8') as file:
        for route_ident, segments in data_dict.items():
            for segment in segments:
                file.write(f"{route_ident} {segment['Segment_Number']} {segment['Fix_Ident']} {segment['Latitude']} {segment['Longitude']}\n")
    print(f"已将NAIP航路添加到 {os.path.abspath(file_path)}")

def sort_data_dict(data_dict):
    sorted_data = {}
    for route_ident in sorted(data_dict.keys()):
        sorted_data[route_ident] = data_dict[route_ident]
    return sorted_data

def order_route(file1):
    # 主程序
    data_dict = read_file_to_dict(file1)
    data_dict = renumber_segments(data_dict)
    data_dict = sort_data_dict(data_dict)
    write_dict_to_file(data_dict, file1)
