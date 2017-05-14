import csv
import datetime
import multiprocessing
import os
import sys
import pandas
from multiprocessing.pool import Pool

from MapReduce import MapReduce

"""
Ziel Teil 1: Throughput pro Minute (Serverseitiges Log)
"""

def task1map(data):
    start_time = datetime.datetime.now()
    print(multiprocessing.current_process().name + " started mapping.")
    timestamps = []
    for index, row in data.iterrows():
        if row['code']=='res_snd':
            timestamps.append( (int(row['time'])//60000, 1) )
    stop_time = datetime.datetime.now()
    running_time = stop_time - start_time
    print(multiprocessing.current_process().name + " finished. Time spent: " + running_time + " ms")
    return timestamps


def task1reduce(timestamps):
    return timestamps

def split_csv(input_file, chunks=multiprocessing.cpu_count()):
    in_file_size = os.path.getsize(input_file)
    print('Input file size: ', in_file_size)
    chunk_size = in_file_size // chunks
    print('Target chunk size: ', chunk_size)
    chunks = []
    for chunk in pandas.read_csv(input_file, chunksize=chunk_size):
        chunks.append(chunk)
    return chunks

if __name__ == '__main__':

    #if (len(sys.argv) < 2):
    #    print("Program requires path to file for reading!")
    #    sys.exit(1)

    print("Number of processors: " + str(multiprocessing.cpu_count()))

    #if (len(sys.argv) > 2):
    #    chunks = sys.argv[2]


    # Load the file passed as argument
    #data = csv.DictReader(open(sys.argv[1]), dialect='excel')

    # Create a pool of processes according to cpu count
    #pool = Pool(processes=multiprocessing.cpu_count())

    chunks = split_csv("C:\\Users\\keRbeRos\\PycharmProjects\\ADMPython\\data\\mw_trace50.csv", multiprocessing.cpu_count())

    mapper = MapReduce(task1map, task1reduce)
    minute_averages = mapper(chunks)
    print(minute_averages)
