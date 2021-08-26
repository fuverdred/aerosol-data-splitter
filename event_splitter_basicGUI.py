from tkinter import filedialog as fd
import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'

DROP_COLS = ['homeid', 'nodetype', 'nodename']

def get_events(events_file, date):
    '''
    Events must be in a specifc format eg.
        17:17:30	pulsed_humidifier
        17:42:57	pulsed_humidifier
        18:18:52	pulsed_humidifier
    Currently the name of the event is ignored.
    '''
    with open(events_file) as f:
        events = [row.split() for row in f.read().split('\n')]
    return [pd.to_datetime(' '.join((date, time[0]))) for time in events]


def clean_data(df):
    '''
    Convert timestamps to useable datetimes,
    remove unecessary data. Return a dataframe sorted by:
        -nodeid
        -sensorname
    to allow easy chopping up of the dataframe in the next step
    '''
    df['timestamp'] = pd.to_datetime(df['timestamp']).dt.tz_localize(None)
    df = df.drop(DROP_COLS, axis=1)  # Remove unneeded columns
    return df


def split_data(raw_filename, output_folder, event_file=None):
    df = pd.read_csv(raw_filename)
    df = clean_data(df)

    if event_file:
        events = get_events(event_file, str(df['timestamp'][0].date()))
        events.append(max(df['timestamp']))

    for node in sorted(df['nodeid'].unique()):
        for sensorname in sorted(df['sensorname'].unique()):
            filename = "/nodeid:{:d}_sensorname:{:s}".format(node, sensorname)
            print("Writing ", filename)

            mask = (df['nodeid'] == node) & (df['sensorname'] == sensorname)
            mini_df = df[mask]
            mini_df = mini_df.sort_values('timestamp')

            if event_file:
                for i, event in enumerate(events[:-1], 1):
                    start, end = event, events[i]
                    mask = (mini_df['timestamp']>start) & (mini_df['timestamp']<end)
                    event_df = mini_df[mask]
                    # Add time in seconds, zero'd at the start
                    time_s = (event_df['timestamp'] - min(event_df['timestamp'])).dt.total_seconds()                    event_df['time_s'] = time_s
                    event_df.to_csv(output_folder + filename +
                                    "_event:{:d}".format(i) + ".csv")
            else:
                mini_df.to_csv(output_folder + filename + '.csv', index=False)


class Exit(Exception): pass
while 1:
    try:
        print("Enter the file to be split")
        input_file = fd.askopenfilename()
        print("Enter a folder to output the split data to")
        output_folder = fd.askdirectory()
        print("Enter a list of event times to split the data into.\n"
              "If not required press cancel")
        events_file = fd.askopenfilename()

        if input_file and output_folder:
            split_data(input_file, output_folder, events_file)

        while 1:
            print("Split more data? y/n")
            answer = input().lower()
            if answer == 'y':
                break
            elif answer == 'n':
                raise Exit
            else:
                print("Enter a valid option")
    except Exit:
        break
