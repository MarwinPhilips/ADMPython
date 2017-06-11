import datetime
import os
import numpy
import pandas


def split_csv(input_file, output_file, chunk_num=2):
    in_file_size = os.path.getsize(input_file)
    print('Input file size: ', in_file_size)
    start_time = datetime.datetime.now()
    df = pandas.read_csv(input_file, skiprows=(1,2,3,4,5,7))
    chunks = numpy.array_split(df, chunk_num)
    for index, chunk in enumerate(chunks):
        chunk.to_csv(output_file + "_" + str(index) + ".csv")
    stop_time = datetime.datetime.now()
    running_time = stop_time - start_time
    print("Chunking done in " + str(running_time))
    return True

# /Users/kerberos/Dropbox/04 Info FH/Advanced Data Management/MapReduce/split_1_4
# /Users/kerberos/Dropbox/04 Info FH/Advanced Data Management/MapReduce/trace_50
input_file = "/Users/kerberos/Dropbox/04 Info FH/Advanced Data Management/MapReduce/trace_50/client_trace50.csv"
output_file = "/Users/kerberos/Dropbox/04 Info FH/Advanced Data Management/MapReduce/split_2_4/client_trace_split"

split_csv(input_file, output_file, 4)