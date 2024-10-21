import os
import difflib

def compare_files(file1, file2):
    with open(file1, 'r', encoding='utf-8') as f1, open(file2, 'r', encoding='utf-8') as f2:
        file1_lines = f1.readlines()
        file2_lines = f2.readlines()

    diff = difflib.unified_diff(file1_lines, file2_lines, fromfile=file1, tofile=file2, lineterm='')
    differences = list(diff)
    
    return differences

def compare_directories(dir1, dir2):
    differences = []
    for root1, _, files1 in os.walk(dir1):
        for file1 in files1:
            file1_path = os.path.join(root1, file1)
            file2_path = file1_path.replace(dir1, dir2, 1)
            if os.path.exists(file2_path):
                diffs = compare_files(file1_path, file2_path)
                if diffs:
                    differences.extend(diffs)
            else:
                differences.append(f"文件 {file2_path} 不存在。")
    
    for root2, _, files2 in os.walk(dir2):
        for file2 in files2:
            file2_path = os.path.join(root2, file2)
            file1_path = file2_path.replace(dir2, dir1, 1)
            if not os.path.exists(file1_path):
                differences.append(f"文件 {file1_path} 不存在。")
    
    return differences

if __name__ == "__main__":
    dir1 = r"D:\Microsoft Flight Simulator\Community\ifly-aircraft-737max8\Data\navdata\Permanent\WPNAVRTE.txt"
    dir2 = r"D:\yyz\Documents\VisualStudioCode\iFly-NDB\raw\WPNAVRTE.txt"
    differences = compare_directories(dir1, dir2)
    
    if differences:
        print('两文件夹存在不同，以下是不同：')
        for line in differences:
            print(line)
    else:
        print('两文件夹完全相同。')