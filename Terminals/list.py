import warnings
warnings.filterwarnings('ignore')
import os
import pandas as pd
import re
from Terminals.merged_data import generate_merged_data

def generate_transitions(data):

    for row in data.itertuples(index=False):
        if row.Type in ('6', 'A'):
            yield {
                'Proc': row.Type.strip(),
                'ICAO': row.ICAO,
                'Name': row.Transition,
                'Rwy': row.Terminal
            }
    
def get_terminals(conn, start_terminal_id, end_terminal_id):
    
    merged_data = generate_merged_data(conn, start_terminal_id, end_terminal_id)
    
    # 使用生成器来生成符合条件的记录
    transitions = list(generate_transitions(merged_data))
    
    # 将生成的记录转换为DataFrame
    transitions = pd.DataFrame(transitions)
    
    # 读取符合条件的Terminals表格，并过滤起始 TerminalID
    terminals = pd.read_sql_query(f"""
        SELECT Proc, ICAO, Name, Rwy
        FROM Terminals
        WHERE ID BETWEEN {start_terminal_id} AND {end_terminal_id}
    """, conn)
    
    # 关闭数据库连接
    conn.close()
    
    # 过滤出不含数字的ICAO
    terminals = terminals[~terminals['ICAO'].str.contains(r'\d')]
    
    # 合并字典
    terminals = pd.concat([transitions, terminals], ignore_index=True)
    
    # 确保输出目录存在
    os.makedirs('output/SID', exist_ok=True)
    os.makedirs('output/STAR', exist_ok=True)
    
    return terminals, merged_data

# 函数：解析已存在文件并提取信息
def parse_existing_file(filename):
    if not os.path.exists(filename):
        return {}, 1
    
    with open(filename, 'r') as f:
        lines = f.readlines()
    
    proc_dict = {}
    seqn = 1
    for line in lines:
        if line.startswith("[list]"):
            continue
        if line.startswith("["):
            break
        
        match = re.match(r"Procedure\.(\d+)=(\S+)\.(\S+)", line)
        if match:
            seqn = int(match.group(1))
            name_rwy = f"{match.group(2)}.{match.group(3)}"
            proc_dict[name_rwy] = seqn
    
    return proc_dict, seqn + 1

def write_to_file(icao, proc, data):
    if proc == 2:
        filename = f"output/SID/{icao}.sid"
    elif proc == 1:
        filename = f"output/STAR/{icao}.star"
    elif proc == 3:
        filename = f"output/STAR/{icao}.app"
    elif proc == '6':
        filename = f"output/SID/{icao}.sidtrs"
    elif proc == 'A':
        filename = f"output/STAR/{icao}.apptrs"
    else:
        return

    proc_dict, seqn = parse_existing_file(filename)
    
    if not os.path.exists(filename):
        with open(filename, 'w') as f:
            f.write("")

    with open(filename, 'r+') as f:
        lines = f.readlines()

        # 找到第二个以 '[' 开头的行的位置
        second_bracket_index = None
        for i in range(1, len(lines)):
            if lines[i].startswith("["):
                second_bracket_index = i
                break

        # 清空第二个以 '[' 开头的行之前的内容
        if second_bracket_index is not None:
            lines = lines[second_bracket_index:]

        # 在第一行插入新的 [list]
        lines.insert(0, "[list]\n")

        # 将新内容插入到 [list] 行和第二个以 '[' 开头的行之间
        new_lines = []
        prev_proc = prev_icao = None
        for index, row in data.iterrows():
            name_rwy = f"{row['Name']}.{str(row['Rwy']).zfill(2)}"
            
            if name_rwy not in proc_dict:
                if prev_proc == proc and prev_icao == icao:
                    seqn += 1
                else:
                    seqn = proc_dict[name_rwy] if name_rwy in proc_dict else seqn
                
                prev_proc = proc
                prev_icao = icao
                proc_dict[name_rwy] = seqn

        # 构建新字典内容
        for name_rwy, idx in proc_dict.items():
            proc, rwy = name_rwy.split('.')
            procedure_line = f"Procedure.{idx}={proc}.{rwy}\n"
            new_lines.append(procedure_line)

        # 插入新内容
        lines[1:1] = new_lines
        if lines[-1].endswith("\n"):
            lines[-1] = lines[-1].rstrip("\n")
        # 写回文件
        f.seek(0)
        f.truncate()
        f.writelines(lines)
        
def list_generate(conn, start_terminal_id, end_terminal_id):
    terminals, merged_data = get_terminals(conn, start_terminal_id, end_terminal_id)
    for icao in terminals['ICAO'].unique():
        for proc in [1, 2, 3, '6', 'A']:
            data = terminals[(terminals['ICAO'] == icao) & (terminals['Proc'] == proc)]
            if not data.empty:
                write_to_file(icao, proc, data)
    return merged_data
