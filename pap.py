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
        for data_id, data_val in data_list:
            sorted_data[data_id].append(data_val)

    return sorted_data


def plex_sorter(data):
    """
    Accepts a singular simple_mapper output: [(<key1>, 1), (<key2>, 1)...]
    Returns a dict of key & list of tuple's second value: { key1: [1,1,...],...}
    """
    sorted_data = defaultdict(list)

    for data_id, data_val in data:
        sorted_data[data_id].append(data_val)

    return sorted_data


def plex_reducer(kv_pair):
    return {kv_pair[0]: sum(kv_pair[1])}


def simple_output(data):
    import pprint
    pprint.pprint(data)


def map_pool_data(func, iter_data):
    pool_size = multiprocessing.cpu_count() * 2
    pool = multiprocessing.Pool(processes=pool_size)

    pool_data = pool.map(func, iter_data)
    pool.close()
    pool.join()

    return pool_data


def get_input_stream(inputs):
    if not inputs:
        return (('Java', 'Hadoop', 'RDBMS', 'Prolog', 'Lisp', 'Pascal',),
                ('Java', 'Java', 'RDBMS', 'Prolog', 'Prolog',),
                ('Java', 'Hadoop', 'RDBMS', 'Prolog', 'Lisp', 'Pascal'),
                )
    else:
        raise Exception('Input situation not addressed.')


def map_data(streams, mapper):
    """
    Accepts list of data streams
    Returns list of - list of tuples
    """
    mapped_data = map_pool_data(mapper, streams)
    return mapped_data


def plex_merge_dicts(lock, data_dict, plex_dict):
    with lock:
        keys = data_dict.keys()
        for key in keys:
            # print("Process: %s, Plex Dict: %s, Data Dict: %s, Key: %s" %(multiprocessing.current_process().name, plex_dict, data_dict, key))
            # print("%s: %s <- %s:: %s" %(multiprocessing.current_process().name, plex_dict, key, data_dict[key]))

            # Get shared list
            lst = plex_dict[key]
            value = data_dict[key]
            # Update list
            lst+= value
            # forces the shared list to be serialized back to manager
            plex_dict[key]=lst
            # Solution: http://stackoverflow.com/a/8644552


def plex_sort_data(mdata, sorter):

    sorted_dicts = map_pool_data(sorter, mdata)
    manager = multiprocessing.Manager()
    sorted_data = manager.dict()
    keys = set()

    for dct in sorted_dicts:
        for key in dct.keys():
            sorted_data[key] = list()

    processes = []

    lock = multiprocessing.Lock()
    for dct in sorted_dicts:
        p = multiprocessing.Process(target=plex_merge_dicts, args=(lock, dct,sorted_data))
        processes.append(p)
        p.start()

    for p in processes:
        p.join()
    return sorted_data


def sort_data(mdata, sorter):
    """
    Accepts list of - list of tuples
    Returns dict of keys & mapper's tuple value
    """
    # pool_size = multiprocessing.cpu_count() * 2
    # pool = multiprocessing.Pool(processes=pool_size)
    sorted_dicts = map_pool_data(sorter, mdata)
    sorted_data = {}

    for sorted_dict in sorted_dicts:
        sdict_keys = sorted_dict.keys()
        for key in sdict_keys:
            sdata_keys = sorted_data.keys()
            if key in sdata_keys:
                sorted_data[key].extend(sorted_dict[key])
            else:
                sorted_data[key] = sorted_dict[key]

    return sorted_data


def reduce_data(sdata, reducer):
    reduced_data = dict()
    reduced_list = map_pool_data(reducer, sdata.items())

    for item in reduced_list:
        reduced_data.update(item)

    return reduced_data


def start(input_files=None, mapper=simple_mapper, sorter=plex_sorter,
          reducer=plex_reducer, output=simple_output):
    input_stream = get_input_stream(input_files)
    mapped_data = map_data(input_stream, mapper)
    # sorted_data = sort_data(mapped_data, sorter)
    sorted_data = plex_sort_data(mapped_data, sorter)
    reduced_data = reduce_data(sorted_data, reducer)
    assert reduced_data == {
        'Java': 4, 'Hadoop': 2, 'RDBMS': 3, 'Prolog': 4, 'Lisp': 2, 'Pascal': 2}
    # output(reduced_data)


if __name__ == '__main__':
    for i in range(0,50):
        start()
        print('.', end=' ')
