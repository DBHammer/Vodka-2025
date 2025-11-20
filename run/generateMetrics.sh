#!/bin/bash

/usr/bin/python3 draw.py

# 在 b.csv 文件开头写入一行
echo "date,type,path,delay,filter_ratio,line" | cat - result/delay/tpchresult-3.csv >temp && mv temp result/delay/tpchresult-3.csv
echo "date,type,path,delay,filter_ratio,line" | cat - result/delay/tpchresult-4.csv >temp && mv temp result/delay/tpchresult-4.csv
echo "date,type,path,delay,filter_ratio,line" | cat - result/delay/tpchresult-5.csv >temp && mv temp result/delay/tpchresult-5.csv
echo "date,type,path,delay,filter_ratio,line" | cat - result/delay/tpchresult-6.csv >temp && mv temp result/delay/tpchresult-6.csv

# 执行 c.py 文件
/usr/bin/python3 delayModeling.py

# 删除 b.csv 文件的第一行标题
sed -i '1d' result/delay/tpchresult-3.csv
sed -i '1d' result/delay/tpchresult-4.csv
sed -i '1d' result/delay/tpchresult-5.csv
sed -i '1d' result/delay/tpchresult-6.csv

# 执行 d.sh 文件
./generateReport.sh
