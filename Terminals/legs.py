import warnings
warnings.filterwarnings('ignore')
import os
import pandas as pd
import re
from Terminals.list import list_generate
import concurrent.futures
import time

def get_terminal_ids():
    while True:
        user_input = input("请输入要转换终端程序集的起始TerminalID和结束TerminalID，用空格分隔二者：")
        terminal_ids = user_input.split()
        if len(terminal_ids) == 1 and terminal_ids[0].isdigit():
            start_terminal_id = int(terminal_ids[0])
            print("终止ID未输入，将自动转换到数据库中最后一个终端程序")
            end_terminal_id = 999999999  # 设定终止ID为999999999
            return start_terminal_id, end_terminal_id
        elif len(terminal_ids) == 2 and all(id.isdigit() for id in terminal_ids):
            start_terminal_id = int(terminal_ids[0])
            end_terminal_id = int(terminal_ids[1])
            return start_terminal_id, end_terminal_id
        else:
            print("请输入有效的数字，并用空格分隔！")

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

def terminals(conn):
    # 获取用户指定的起止 TerminalID
    start_terminal_id, end_terminal_id = get_terminal_ids()
    start_time = time.time()
    # 建立航段字典用于查询
    data = list_generate(conn, start_terminal_id, end_terminal_id)
    with concurrent.futures.ProcessPoolExecutor() as executor:
        futures = []
        for root, dirs, files in os.walk("output"):
            for file in files:
                futures.append(executor.submit(process_file, file, root, data))
        for future in concurrent.futures.as_completed(futures):
            future.result()
    end_time = time.time()
    run_time = end_time - start_time
    print(f"终端数据转换完毕，用时：{round(run_time,3)}秒")