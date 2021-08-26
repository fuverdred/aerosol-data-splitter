import os

import pandas as pd
import matplotlib.pyplot as plt

os.chdir('example_output')


def split_filename(file):
    return dict([vals.split('=') for vals in file.strip('.csv').split('_')])    

nodeid = '1'
event = '3'
files = [f for f in os.listdir() if
         split_filename(f)['nodeid'] == nodeid and
         split_filename(f)['event'] == event]

for f in files:
    node, sensor, event = f.strip('.csv').split('_')
    if "Bin" in split_filename(f)['sensorname']:
        df = pd.read_csv(f)
        plt.plot(df['time_s'],
                 df['reading'],
                 label=sensor)
        plt.title(', '.join((node, event)))
        plt.legend()
        plt.xlabel('Time (s)')
        plt.ylabel('Some units')
plt.show()
