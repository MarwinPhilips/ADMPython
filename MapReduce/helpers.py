import multiprocessing
import os
import pandas

def split_csv(input_file, chunks=multiprocessing.cpu_count()):
    in_file_size = os.path.getsize(input_file)
    print('Input file size: ', in_file_size)
    chunk_size = in_file_size // chunks
    print('Target chunk size: ', chunk_size)
    chunks = []
    for chunk in pandas.read_csv(input_file, chunksize=chunk_size):
        chunks.append(chunk)
    return chunks