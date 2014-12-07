from collections import defaultdict


def simple_mapper(stream):
    return map(lambda key: (key, 1), stream)


def simple_sorter(data):
    sorted_data = defaultdict(list)
    for data_list in data:
        # map(lambda k,v: sorted_data[k].append(v), data_list)
        [sorted_data[data_id].append(data_val)
         for data_id, data_val in data_list]
    return sorted_data


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
    if not mapper:
        mapper = simple_mapper

    mapped_data = list(map(mapper, streams))

    # for stream in streams:
    # mappend_data.append(map_process_to_core(stream, mapper))

    return mapped_data


def sort_data(mdata, sorter):
    if not sorter:
        sorter = simple_sorter

    sorted_data = sorter(mdata)

    return sorted_data


def start(input_files, mapper, sorter, reducer, output):
    input_stream = get_input_stream(input_files)
    mapped_data = map_data(input_stream, mapper)
    sorted_data = sort_data(mapped_data, sorter)
    reduced_data = reduce_data(sorted_data, reducer)
    output(reduced_data)


if __name__ == '__main__':
    start(input_files=None, mapper=None,
          sorter=None, reducer=None, output=None)
