import matplotlib.pyplot as plt
import time
import os
import datetime
from datetime import datetime as datetime2

BASE_DIR = "/home/xjk/hzr/Vodka-Benchmark/run"
DIR_NAME = "/result"
CSV_FILE_DIR = BASE_DIR + DIR_NAME + "/"
# CSV_FILE_NAME = ['tpchresult-3.csv']
# CSV_FILE_NAME = ['tpchresult-3.csv', 'tpchresult-4.csv']
CSV_FILE_NAME = ['tpchresult-4-14:00.csv', 'tpchresult_pg_cluster.csv']
OFFSET = 0
QUERYNUMBER = 22
WAREHOUSE = 20
TPTHREADS = 20
APTHREADS = 1


def generate_result():
    print("[read csv data]")
    total_data = []
    for csv_file_name in CSV_FILE_NAME:
        csv_file_path = CSV_FILE_DIR + csv_file_name
        headers = ""
        data = [[] for _ in range(QUERYNUMBER + 2)]
        with open(csv_file_path) as f:
            headers = next(f)
            print(headers)
            for row in f:
                cur = row.split(',')
                query_id = int(cur[1])
                date = cur[0][:19]
                print(date)
                try:
                    timeArray = time.strptime(date, "%Y-%m-%d %H:%M:%S")
                except:
                    print("date = ", date)
                start_time = time.mktime(timeArray)
                latency = round((float(cur[3])), 1)
                data[query_id].append([start_time, latency])
        total_data.append(data)

    print("[analyse]")
    avg_latencys = [[] for _ in range(QUERYNUMBER + 1)]
    for data in total_data:
        for i in range(1, QUERYNUMBER + 1):
            data[i].sort(key=lambda k: k[0])
        latencys = [0 for _ in range(QUERYNUMBER + 1)]
        for i in range(1, QUERYNUMBER + 1):
            for d in data[i]:
                latencys[i] += d[1]
        for i in range(1, QUERYNUMBER + 1):
            avg_latencys[i].append(latencys[i] / len(data[i]))

    plt.rc('font', size='6', weight='bold')
    fig, ax = plt.subplots(5, 5, figsize=(12, 7))

    set_flag = False
    colors = ['blue', 'green', 'red', 'orange']
    legends = ['OB', 'PG', 'TiDB', 'PolarDB']
    pos = 0
    height = [0 for _ in range(QUERYNUMBER + 1)]
    for k, data in enumerate(total_data):
        l = [len(data[i]) for i in range(QUERYNUMBER + 1)]
        x = [[] for _ in range(QUERYNUMBER + 1)]
        y = [[] for _ in range(QUERYNUMBER + 1)]
        start_time = [data[i][0][0] if len(data[i]) > 0 else 0 for i in range(0, QUERYNUMBER + 1)]
        for i in range(1, QUERYNUMBER + 1):
            p = 0
            while p < l[i]:
                if data[i][p][0] - start_time[i] >= OFFSET:
                    x[i].append(data[i][p][0] - start_time[i])
                    y[i].append(data[i][p][1])
                    height[i] = max(height[i], data[i][p][1])
                p += 1

        idx = 1
        print("[draw]")
        x_labels = ['Q' + str(i) for i in range(0, QUERYNUMBER + 1)]
        wen = ['Q2', 'Q11', 'Q16']
        has_date = ['Q1', 'Q3', 'Q4', 'Q5', 'Q6', 'Q7', 'Q8', 'Q10', 'Q12', 'Q14', 'Q20']
        #         print("start_time = ", start_time)
        for i in range(5):
            for j in range(5):
                if idx > QUERYNUMBER:
                    continue
                ax[i, j].plot(x[idx], y[idx], color=colors[k], linewidth=1, linestyle='-', alpha=0.6,
                              label=legends[pos], marker=".")
                ax[i, j].legend(bbox_to_anchor=(0.02, 0.98), loc='upper left', ncol=2, borderaxespad=0.)
                if x_labels[idx] in wen:
                    ax[i, j].set_xlabel(xlabel=x_labels[idx], color='blue', size='6', weight="bold")  # 数据规模不变化
                elif x_labels[idx] in has_date:
                    ax[i, j].set_xlabel(xlabel=x_labels[idx], color='red', size='6', weight="bold")  # 数据规模变化且含有时间字段
                else:
                    ax[i, j].set_xlabel(xlabel=x_labels[idx], color='green', size='6', weight="bold")  # 数据规模变化且不含有时间字段
                ax[i, j].set_ylabel(ylabel='Latency(ms)', size='6', weight="bold")
                ax[i, j].grid(alpha=0.3)
                idx += 1

        plt.subplots_adjust(left=None, bottom=None, right=None, top=None, wspace=0.75, hspace=0.75)
        pos += 1

    idx = 1
    title = 'Experiment under ' + str(WAREHOUSE) + ' warehouses, ' + str(TPTHREADS) + ' tp threads, ' + str(
        APTHREADS) + ' ap threads'
    fig.suptitle(title, fontsize=13)
    for i in range(5):
        for j in range(5):
            if idx > QUERYNUMBER:
                continue
            ax[i, j].set_ylim(0, height[idx] * 2)
            idx += 1
    if (len(CSV_FILE_NAME) >= 2):
        plt.savefig("vodka_result/query-all.png", dpi=200)
    else:
        plt.savefig("vodka_result/query-single.png", dpi=200)
    plt.show()


if __name__ == "__main__":
    generate_result()
