import warnings
warnings.filterwarnings('ignore')

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

def compare_and_insert(file1_dict, file2_dict):
    for route_ident in file2_dict:
        if route_ident in file1_dict:
            file1_segments = file1_dict[route_ident]
            file2_segments = file2_dict[route_ident]
            first_match = next((item for item in file1_segments if item['Fix_Ident'] == file2_segments[0]['Fix_Ident']), None)
            last_match = next((item for item in reversed(file1_segments) if item['Fix_Ident'] == file2_segments[-1]['Fix_Ident']), None)
            if first_match and last_match:
                start_index = file1_segments.index(first_match)
                end_index = file1_segments.index(last_match)
                file1_segments[start_index:end_index+1] = file2_segments
            else:
                file1_segments.extend(file2_segments)
        else:
            file1_dict[route_ident] = file2_dict[route_ident]

def delete_lines(file_dict):
    keys_to_delete = {key for key in file_dict if key[0] in {'A', 'B', 'G', 'L', 'M', 'R', 'V', 'W'}}
    for key in keys_to_delete:
        del file_dict[key]

def save_to_file(file_path, data_dict):
    with open(file_path, 'w', encoding='utf-8') as file:
        for route_ident, entries in data_dict.items():
            for entry in entries:
                line = f"{route_ident} {entry['Segment_Number']:03} {entry['Fix_Ident']} {entry['Latitude']} {entry['Longitude']}\n"
                file.write(line)

def save_sample_to_file(file_path):
    sample_text = ''';Supplemental Navaid Database (Option)
;;
;Data format is same as P3D_root\\iFly\\737MAX\\navdata\\
;;
;If any route in this file have same identifier as in
;Main Navaid Database, FMC will delete route data in
;the Main Navaid Database
;;
;This is a sample file
;-------------------------------------------------------------
TEST 001 TEST1 33.114350 139.788483
TEST 002 TEST2 33.193211 138.972397
TEST 003 TEST3 33.447742 135.794495
'''
    with open(file_path, 'w') as file:
        file.write(sample_text)

def insert_route(file1, file2):
    file1_dict = read_file_to_dict(file1)
    file2_dict = read_file_to_dict(file2)
    
    # Compare and insert lines
    compare_and_insert(file1_dict, file2_dict)
    
    # Save changes to file1
    save_to_file(file1, file1_dict)
    
    # Save sample text to file2
    save_sample_to_file(file2)
