import multiprocessing

import collections

import itertools


class MapReduce(object):

    def __init__(self, map_function, reduce_function, worker_count=None):
        """
        :param map_function: 
        :param reduce_function: 
        :param worker_count: 
        """
        self.map_function = map_function
        self.reduce_function = reduce_function
        self.pool = multiprocessing.Pool(worker_count)

    def shuffle(self, mapped_values):
        """
        :param mapped_values: 
        :return: 
        """
        shuffeled_data = collections.defaultdict(list)
        for key, value in mapped_values:
            shuffeled_data[key].append(value)
        return shuffeled_data.items()

    def __call__(self, inputs, chunksize=1):
        """
        :param inputs: 
        :param chunksize: 
        :return: 
        """
        map_responses = self.pool.map(self.map_function, inputs, chunksize=chunksize)
        shuffeled_data = self.shuffle(itertools.chain(*map_responses))
        reduced_values = self.pool.map(self.reduce_function, shuffeled_data)
        return reduced_values