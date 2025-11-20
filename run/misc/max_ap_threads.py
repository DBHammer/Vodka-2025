import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

if __name__ == '__main__':
    # create DataFrame
    plt.figure(figsize=(10, 6))
    df = pd.DataFrame(
        {'Database': ['OceanBase', 'TiDB', 'Postgres', 'PolarDB', 'OceanBase', 'TiDB', 'Postgres', 'PolarDB',
                      'OceanBase', 'TiDB', 'Postgres', 'PolarDB', 'OceanBase', 'TiDB', 'Postgres', 'PolarDB'],
         'Max AP Threads Number': [50, 46, 49, 1,
                                   54, 33, 46, 1,
                                   49, 20, 12, 1,
                                   49, 0, 7, 1],
         'TpmC': ['10000', '10000', '10000', '10000', '15000',
                  '15000', '15000', '15000', '20000', '20000',
                  '20000', '20000', '25000', '25000', '25000', '25000']})
    # set seaborn plotting aesthetics
    sns.set(style='white')
    # create grouped bar chart
    sns.barplot(x='TpmC', y='Max AP Threads Number', hue='Database', data=df,
                palette=['steelblue', 'firebrick', sns.xkcd_rgb["pale red"], 'orange'])
    # add overall title
    # add axis titles
    plt.xlabel('Different HTAP Databases')
    plt.ylabel('Number of Maximum Support AP Threads')
    plt.legend(loc='best')
    # rotate x-axis labels
    plt.show()
