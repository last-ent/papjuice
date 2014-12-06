def get_input_stream(inputs):
    if not inputs:
        return (('Java', 'Hadoop', 'RDBMS', 'Prolog', 'Lisp', 'Pascal',),
                ('Java', 'Java', 'RDBMS', 'Prolog', 'Prolog',),
                ('Java', 'Hadoop', 'RDBMS', 'Prolog', 'Lisp', 'Pascal'),
               )    
    else:
        raise Exception('Input situation not addressed.')


def start(input_files, mapper, sorter, reducer, output):
    input_stream = get_input_stream(input_files)
    mapped_data = map_data(input_stream, mapper)
    sorted_data = sort_data(mapped_data, sorter)
    reduced_data = reduce_data(sorted_data, reducer)
    output(reduced_data)


if __name__=='__main__':
    start(input_files=None, mapper=None, sorter=None, reducer=None, output=None)
