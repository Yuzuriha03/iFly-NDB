import os
import re
import time
import shutil
import warnings
import pandas as pd
import concurrent.futures
from Terminals.list import list_generate

warnings.filterwarnings('ignore')

def parse_files(file, root):
    procedures = {}
    details = {}
    if file.endswith(('.app', '.apptrs', '.sid', '.sidtrs', '.star')):
        icao = file.split('.')[0]
        with open(os.path.join(root, file), 'r', encoding='utf-8') as f:
            lines = f.readlines()
            proc_dict = []
            detail_set = set()  # 使用集合确保唯一性
            list_started = False
            for line in lines:
                if line.startswith("[list]"):
                    list_started = True
                elif list_started and not line.startswith("["):
                    match = re.match(r"Procedure\.(\d+)=(\S+)\.(\S+)", line)
                    if match:
                        transition = match.group(2)
                        via = match.group(3)
                        proc_dict.append(f"{icao}.{transition}.{via}")
                elif list_started and line.startswith("["):
                    break
            procedures[icao] = proc_dict
            # 找出每个文件的以[开头，以]结束的行
            for line in lines:
                if line.startswith("[") and line.endswith("]\n"):
                    match_detail = re.match(r"\[(\S+)\.(\S+)\.(\d+)\]", line)
                    if match_detail:
                        transition = match_detail.group(1)
                        via = match_detail.group(2)
                        detail_set.add(f"{transition}.{via}")
            details[icao] = list(detail_set)  # 将集合转换为列表
    return procedures, details

def legs_generate(icao, procedures, details, data):
    results = []
    current_transition = None
    seqno = 1
    for index, row in data.iterrows():
        if row['ICAO'] == icao:
                if row['Type'] == '6' or row['Type'] == 'A':
                    transition = row['Transition']
                    via = row['Terminal']
                else:
                    transition = row['Terminal']
                    via = str(row['Rwy']).zfill(2)
                Procedure = f"{row['ICAO']}.{transition}.{via}"
                Name = f"{transition}.{via}"
                if Procedure in procedures.get(row['ICAO'], {}):
                    #print("{row['ICAO']} {Procedure}存在匹配程序")
                    if Name not in details.get(row['ICAO'], {}):
                        #print("{row['ICAO']} {Procedure}的相关航段未写入")
                        # 如果 Terminal 更新，重置 seqno
                        if transition != current_transition:
                            current_transition = transition
                            seqno = 1
                        else:
                            seqno += 1
                        # 创建格式化字符串，并忽略 NaN、None 或空格的列
                        row_str = f"[{transition}.{via}.{seqno}]\n"
                        for col in row.index:
                            if col not in ['ICAO', 'Rwy', 'Terminal', 'Transition', 'Type']:
                                value = row[col]
                                if pd.notnull(value) and value != '':
                                    row_str += f"{col}={value}\n"
                        results.append(row_str.strip())  # 去掉最后的空行
    return results

def process_file(file, root, data):
    icao = file.split('.')[0]
    procedures, details = parse_files(file, root)
    results = legs_generate(icao, procedures, details, data)
    filepath = os.path.join(root, file)
    with open(filepath, 'r+', encoding='utf-8') as f:
        lines = f.readlines()
        # 不再需要找到插入点，直接在文件的最后插入
        lines.append("\n")
        for result in results:
            lines.append(result + "\n")
        f.seek(0)
        f.truncate()
        f.writelines(lines)

def copy_file_if_not_exists(src_file, dest_file):
    if os.path.exists(dest_file):
        return  # 如果Supplemental目录下已存在同名文件则跳过
    os.makedirs(os.path.dirname(dest_file), exist_ok=True)
    shutil.copy(src_file, dest_file)
    
def process_files(root, files, permanent_path, supplemental_path_base):
    icao_prefixes = ('VQPR', 'ZB', 'ZG', 'ZH', 'ZJ', 'ZL', 'ZP', 'ZS', 'ZU', 'ZW')
    allowed_extensions = ('.sid', '.sidtrs', '.app', '.apptrs', '.star', '.startrs')
    for file in files:
        if file.startswith(icao_prefixes) and file.endswith(allowed_extensions):
            relative_path = os.path.relpath(os.path.join(root, file), permanent_path)
            supplemental_path = os.path.join(supplemental_path_base, relative_path)
            copy_file_if_not_exists(os.path.join(root, file), supplemental_path)

def terminals(conn, navdata_path, start_terminal_id, end_terminal_id):
    start_time = time.time()
    permanent_path = os.path.join(navdata_path, "Permanent")
    supplemental_path_base = os.path.join(navdata_path, 'Supplemental')
    with concurrent.futures.ProcessPoolExecutor() as executor:
        futures = []
        for root, _, files in os.walk(permanent_path):  # 把现有的进离场数据复制到Supplemental目录下
            futures.append(executor.submit(process_files, root, files, permanent_path, supplemental_path_base))
        for future in concurrent.futures.as_completed(futures):
            future.result()
    # 建立航段字典用于查询
    data = list_generate(conn, start_terminal_id, end_terminal_id, navdata_path)
    with concurrent.futures.ProcessPoolExecutor() as executor:
        futures = []
        for root, dirs, files in os.walk(f"{navdata_path}\\Supplemental"):
            for file in files:
                    futures.append(executor.submit(process_file, file, root, data))
        for future in concurrent.futures.as_completed(futures):
            future.result()
    end_time = time.time()
    run_time = end_time - start_time
    print(f"终端数据转换完毕，用时：{round(run_time,3)}秒")