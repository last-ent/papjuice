#PapJuice

*MapReduce through Python Goggles*

## Inspiration
To learn & practice concepts of MapReduce (based on Hadoop) & MultiProcessing.

## Plan
We will approach the problem in two phases:
* Create a Single Process solution.
* Convert it to Multi Process.

Step 1 ensures that we have correct solution that we can verify against Step 2.

## Concept of MapReduce via cliché - Word Count
Consider we have three documents and we want to find the collective count of unique words in them. We can use MapReduce to take advantage of distributed computing.

If we were to use a production level MapReduce solution we would be using Hadoop or Compute, but since our attempt is to learn MultiProcessing, we will be creating our own MapReduce solution. This way, we also learn about the various components of MapReduce.

MapReduce consists of five major components (in most simplistic terms):
* Input Streams
* Mapper
* Sorter
* Reducer
* Output Stream

```python
def start(input_files=None, mapper=simple_mapper, sorter=simple_sorter,
          reducer=simple_reducer, output=simple_output):
    input_stream = get_input_stream(input_files)
    mapped_data = map_data(input_stream, mapper)
    sorted_data = sort_data(mapped_data, sorter)
    reduced_data = reduce_data(sorted_data, reducer)
    output(reduced_data)

if __name__ == '__main__':
    start()
 ```
## Understanding MapReduce Components 

### Input Streams
For initial development purpose, we will use a list of three String Arrays of known word count. 
From application standpoint, it just expects an iterator of number of input streams, where each input stream is an iterator.
```python
def get_input_stream(inputs):
    if not inputs:
        return (('Java', 'Hadoop', 'RDBMS', 'Prolog', 'Lisp', 'Pascal',),
                ('Java', 'Java', 'RDBMS', 'Prolog', 'Prolog',),
                ('Java', 'Hadoop', 'RDBMS', 'Prolog', 'Lisp', 'Pascal'),
                )
    else:
        raise Exception('Input situation not addressed.')
```

### Mapper
Our Mapper is a function that takes a given data stream and creates mapped data that will be used by Reducer to count words. The mapped data will be a list of tuples (\<word>, 1) where each \<word> can be repeated multiple times in the list.
```python
def map_data(streams, mapper):
    """
    Accepts list of data streams
    Returns list of - list of tuples
    """
    mapped_data = list(map(mapper, streams))
    return mapped_data

def simple_mapper(stream):
    """
    Returns a simple list of tuples: [(<key1>, 1), (<key2>, 1)...]
    based on data in stream
    """
    return map(lambda key: (key, 1), stream)
```

### Sorter
Given that we have three input streams, each input stream will have a list of tuples and each word can be repeated multiple times. We want to ensure that same word is sent to a single Reducer and it is Sorter's responsibility to ensure this happens. 

Sorter achieves this by clubbing together tuple values (\<word>, *value =* 1) for each distinct word. A HashMap is a good way to achieve this.

```python
def sort_data(mdata, sorter):
    """
    Accepts list of - list of tuples
    Returns dict of keys & mapper's tuple value
    """
    sorted_data = sorter(mdata)
    return sorted_data
    
def simple_sorter(data):
    """
    Accepts list of simple_mapper outputs: [ [(<key1>, 1), (<key2>, 1)...], ... ]
    Returns a dict of key & list of tuple's second value: { key1: [1,1,...],...}
    """
    sorted_data = defaultdict(list)
    for data_list in data:
        [sorted_data[data_id].append(data_val)
         for data_id, data_val in data_list]
    return sorted_data
```

### Reducer
Reducer takes each Key-Value pair from Sorter's HashMap and sums up the Value for each Key. Sends back another HashMap and this forms our Output.

```python
def reduce_data(sdata, reducer):
    reduced_data = {}
    for key, value in sdata.items():
        reduced_data.update(reducer(key, value))
    return reduced_data

def simple_reducer(key, values):
    return {key: sum(values)}
```
### Output Stream
We send our Reducer output to Output Stream. We use ```print```, but it can easily be substituted with a different function.

```python
def simple_output(data):
    import pprint
    pprint.pprint(data)
```
___

> ## Code Design
> If we go through above code, we realize that though we described responsibility of each Component - Each Component is pretty much split in two functions. The reason is we are following Single Responsibility principle. This way it will be easier to swap functions at granular level and thus we need not re-write major part of logic everytime we feel a need to change something.