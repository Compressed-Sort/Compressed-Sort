import numpy as np
import pandas as pd
import time
from iotdb.Session import Session
from iotdb.utils.IoTDBConstants import TSDataType, TSEncoding, Compressor
from iotdb.utils.Tablet import Tablet
ip = "127.0.0.1"
port_ = "6667"
username_ = "root"
password_ = "root"
session = Session(ip, port_, username_, password_, fetch_size=1024, zone_id="UTC+8")
session.open(False)



df = pd.read_csv('D:/senior/DQ/research/compressed_sort_paper/code/vldb25/Compressed-Sort/datasets/samsung.csv')

time_column = df.iloc[:, 0].tolist()
value_column = df.iloc[:, 1].tolist()
time_column = [int(t) for t in df.iloc[:, 0]]
value_column = [int(v) for v in df.iloc[:, 1]]
maxTime = max(time_column) - min(time_column) + 10000
#print(len(data)) 
measurements_ = ["status"]
Fvalues_ = [False]
Tvalues_ = [True]
data_types_ = [
    TSDataType.INT64
]

print('tatal size:', len(time_column))

time.sleep(5)  # 等待5秒以确保IoTDB服务已启动

start = time.perf_counter()

# for i in range(0,len(time_column)):
#     session.insert_record("root.ln.wf01.wt01", int(time_column[i]), measurements_, data_types_, [int (value_column[i])])
#     if i % 10000 == 0:
#         print(f"插入了 {i} 条数据")
batch_size = 10000  # 批量插入的大小
repeat_time = 10
for j in range(repeat_time):
    for i in range(0, len(time_column), batch_size):
        batch_times = [t + maxTime*j for t in time_column[i:i + batch_size]]
        batch_values = [[int(v)] for v in value_column[i:i + batch_size]]  # 每行为一组列值

        session.insert_tablet(Tablet(
            device_id="root.ln.wf01.wt01",
            measurements=measurements_,
            data_types=data_types_,
            values=batch_values,
            timestamps=batch_times)
        )

        print(f"批量插入 {min(i + batch_size, len(time_column))} 条数据")


end = time.perf_counter()

print(f"耗时：{end - start} 秒")
throughput = len(time_column) / (end - start)
print(f"吞吐量：{throughput} 项/秒")
# time.sleep(5)
# sql = "FLUSH"
# session.execute_non_query_statement(sql)


