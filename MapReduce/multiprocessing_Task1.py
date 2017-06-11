import csv
import datetime
import multiprocessing
import collections
import pandas
from MapReduce import MapReduce

"""
Ziel Teil 1: Throughput pro Minute (Serverseitiges Log)
"""


def task1map(file):
    start_time = datetime.datetime.now()
    print(multiprocessing.current_process().name + " started mapping.")
    with open(file) as f:
        data = csv.DictReader(f, dialect='excel')
        timestamps = []
        for row in data:
            if row['code']=='res_snd':
                timestamps.append(int(row['time'])//60000)
    stop_time = datetime.datetime.now()
    running_time = stop_time - start_time
    print(multiprocessing.current_process().name + " finished mapping. Time spent: " + str(running_time))
    return timestamps


def task1shuffle(data):
    l = (sorted(list(data)),)
    return l


def task1reduce(timestamps):
    print(multiprocessing.current_process().name + " started reducing.")
    return collections.Counter(timestamps)

if __name__ == '__main__':
    import glob
    start_time = datetime.datetime.now()
    print("Number of processors: " + str(multiprocessing.cpu_count()))
    chunks = glob.glob("E:\\Dropbox\\04 Info FH\\Advanced Data Management\\MapReduce\\split_1_12\\*.csv")
    mapper = MapReduce(task1map, task1reduce, task1shuffle, multiprocessing.cpu_count())
    minute_averages = mapper(chunks)
    df = pandas.DataFrame.from_dict(minute_averages[0], orient='index').reset_index()
    df = df.rename(columns={'index': 'minute', 0: 'count'})
    df.to_csv("E:\\Dropbox\\04 Info FH\\Advanced Data Management\\MapReduce\\multiprocessing_Task1_output.csv")
    stop_time = datetime.datetime.now()
    running_time = stop_time - start_time
    print("Program terminated. Time spent: " + str(running_time))
