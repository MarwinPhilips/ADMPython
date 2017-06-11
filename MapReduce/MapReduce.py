import multiprocessing
import collections


class MapReduce(object):
    def default_shuffle(self, mapped_values):
        """
        Default shuffle if none is provided
        :param mapped_values: dict of mapped values
        :return: shuffeled data
        """
        shuffeled_data = collections.defaultdict(list)
        for key, value in mapped_values:
            shuffeled_data[key].append(value)
        return shuffeled_data.items()


    def __init__(self, map_function, reduce_function, shuffle_function=default_shuffle, worker_count=None):
        """
        Create a MapReduce-object.
        :param map_function: the mapping function to be distributed among processes
        :param reduce_function: the reducing function to be distributed among processes
        :param shuffle_function: single process function for sorting in between distributed tasks
        :param worker_count: maximum number of processes/workers
        """
        self.map_function = map_function
        self.reduce_function = reduce_function
        self.shuffle_function = shuffle_function
        self.pool = multiprocessing.Pool(worker_count)


    def __call__(self, inputs, chunksize=1):
        """
        :param inputs: list of data-parts to be mapped
        :param chunksize: size of chunks to be mapped
        :return: delivers the output as specified in the reduce function
        """
        map_responses = self.pool.map(self.map_function, inputs, chunksize=chunksize)
        shuffeled_data = self.shuffle_function(map_responses)
        reduced_values = self.pool.map(self.reduce_function, shuffeled_data)
        return reduced_values