import os
import csv
import sqlite3
from collections import defaultdict

def process_csv_files():
    # 連接到SQLite資料庫
    conn = sqlite3.connect('id_database.db')
    cursor = conn.cursor()

    # 創建表格（如果不存在）
    cursor.execute('''CREATE TABLE IF NOT EXISTS id_records
                      (id TEXT PRIMARY KEY, count INTEGER)''')

    all_ids = defaultdict(int)
    new_ids = set()
    repeat_ids = set()
    
    # 獲取csv資料夾下所有的CSV文件
    csv_folder = 'csv'
    csv_files = [f for f in os.listdir(csv_folder) if f.endswith('.csv')]
    
    for file in csv_files:
        file_path = os.path.join(csv_folder, file)
        with open(file_path, 'r', newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            
            # 檢查CSV文件是否有'id'列
            if 'id' not in reader.fieldnames:
                print(f"警告: {file} 沒有'id'列")
                continue
            
            # 讀取並添加所有id
            for row in reader:
                id_value = row['id'].strip()
                if id_value:
                    all_ids[id_value] += 1

    # 檢查每個ID是否在資料庫中並更新
    for id_value, count in all_ids.items():
        cursor.execute("SELECT count FROM id_records WHERE id = ?", (id_value,))
        result = cursor.fetchone()
        if result:
            new_count = result[0] + count
            cursor.execute("UPDATE id_records SET count = ? WHERE id = ?", (new_count, id_value))
            repeat_ids.add(id_value)
        else:
            cursor.execute("INSERT INTO id_records (id, count) VALUES (?, ?)", (id_value, count))
            new_ids.add(id_value)

    # 提交變更並關閉連接
    conn.commit()
    conn.close()

    # 寫入new.txt和repeat.txt
    with open('new.txt', 'w', encoding='utf-8') as f:
        f.write('-'.join(sorted(new_ids)))

    with open('repeat.txt', 'w', encoding='utf-8') as f:
        f.write('-'.join(sorted(repeat_ids)))

    # 刪除所有處理過的CSV文件
    for file in csv_files:
        file_path = os.path.join(csv_folder, file)
        os.remove(file_path)
    print(f"已刪除 {len(csv_files)} 個CSV文件")

    return len(all_ids), len(new_ids), len(repeat_ids)

# 執行程序
total_ids, new_count, repeat_count = process_csv_files()
print(f"總共掃描到 {total_ids} 個不重複的ID")
print(f"新增了 {new_count} 個ID")
print(f"重複的ID有 {repeat_count} 個")
print("結果已保存到 new.txt 和 repeat.txt")
