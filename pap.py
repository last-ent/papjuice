from collections import defaultdict
import multiprocessing


def simple_mapper(stream):
    """
    Returns a simple list of tuples: [(<key1>, 1), (<key2>, 1)...]
    based on data in stream
    """
    return list(map(lambda key: (key, 1), stream))


def simple_sorter(data):
    """
    Accepts list of simple_mapper outputs: [ [(<key1>, 1), (<key2>, 1)...], ... ]
    Returns a dict of key & list of tuple's second value: { key1: [1,1,...],...}
    """
    sorted_data = defaultdict(list)
    for data_list in data:
        # map(lambda k,v: sorted_data[k].append(v), data_list)
        [sorted_data[data_id].append(data_val)
         for data_id, data_val in data_list]
    return sorted_data


def simple_reducer(key, values):
    return {key: sum(values)}


def simple_output(data):
    import pprint
    pprint.pprint(data)


def get_input_stream(inputs):
    if not inputs:
        return (('Java', 'Hadoop', 'RDBMS', 'Prolog', 'Lisp', 'Pascal',),
                ('Java', 'Java', 'RDBMS', 'Prolog', 'Prolog',),
                ('Java', 'Hadoop', 'RDBMS', 'Prolog', 'Lisp', 'Pascal'),
                )
    else:
        raise Exception('Input situation not addressed.')


# def map_process_to_core(stream, mapper):


def map_data(streams, mapper):
    """
    Accepts list of data streams
    Returns list of - list of tuples
    """
    pool_size = multiprocessing.cpu_count() * 2
    pool = multiprocessing.Pool(processes=pool_size)

    mapped_data = pool.map(mapper, streams)
    pool.close()
    pool.join()

    return mapped_data


def sort_data(mdata, sorter):
    """
    Accepts list of - list of tuples
    Returns dict of keys & mapper's tuple value
    """
    sorted_data = sorter(mdata)

    return sorted_data


def reduce_data(sdata, reducer):
    reduced_data = {}

    for key, value in sdata.items():
        reduced_data.update(reducer(key, value))

    return reduced_data


def start(input_files=None, mapper=simple_mapper, sorter=simple_sorter,
          reducer=simple_reducer, output=simple_output):
    input_stream = get_input_stream(input_files)
    mapped_data = map_data(input_stream, mapper)
    sorted_data = sort_data(mapped_data, sorter)
    reduced_data = reduce_data(sorted_data, reducer)
    output(reduced_data)


if __name__ == '__main__':
    start()
