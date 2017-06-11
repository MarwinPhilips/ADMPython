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
        data = pandas.read_csv(f, dialect='excel', index_col=0)
        timestamps = []
        for row in zip(data['code'], data['time']):
            if row[0]=='res_snd':
                timestamps.append(int(row[1])//60000)
    print(multiprocessing.current_process().name + " finished mapping. Time spent: " + str(datetime.datetime.now() - start_time))
    return timestamps


def task1shuffle(data):
    start_time = datetime.datetime.now()
    print("Everyday I'm shuffeling...")
    mergedlist =[]
    for list in data:
        mergedlist += list
    l = sorted(mergedlist)
    count = [dict(collections.Counter(l))]
    print("Finished shuffeling. Time spent: " + str(datetime.datetime.now() - start_time))
    return count


def task1reduce(counts):
    # since it makes no sense to use multiprocessing in this reduce task, all reducing is done in the shuffle-function
    return counts

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
