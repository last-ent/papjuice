## Converting PapJuice for Mutilprocessing
When trying to convert a single process into multiprocess, a simple way to look at it is to ask

> How many isolated tasks are we running in sequence, waiting for one task to complete before we can start on the next, while one doesn't have any impact on other?

Such isolated yet queued tasks are a good starting point to implement multiprocessing. If we take a closer look at the code written so far, we can conclude that the following loops can be handled via multiprocessing:
* ```map_data```
* ```simple_sorter```
* ```sort_data```
* ```reduce_data```
* ```simple_reducer```

Let's have a look at how to convert each of these into our required form.

## Concepts of Multiprocessing
This article doesn't aim to be an exhaustive treatise on Multiprocessing but just a brief overview of how to get quick functional code up and running in Python.

On looking at standard examples for Multiprocessing, we notice that the rule of thumb seems to be to have a "Pool" with twice the number processes of the CPUs we have. We use this Pool to map our function and its iterators. Upon completion of mapping our function & iterators to the Pool, we need to ensure that no more tasks are added to it ad-hoc. This is achieved by calling ```close``` function on Pool. Next we need to wait until all the tasks are completed, this being achived by ```join```

So to reiterate:
* Create a Pool of 2x CPUs.
* Map our function & data to the Pool.
* Signal end of task addition.
* Wait for all processes to be completed.

With this in mind, let us look at each of our functions and implement it.

### Map-Data
Consider following code

```python
def map_data(streams, mapper):
    """
    Accepts list of data streams
    Returns list of - list of tuples
    """
    mapped_data = list(map(mapper, streams))
    return mapped_data
```

We notice that ```mapped_data``` is going sequentially over streams (which is defined by ```get_input_stream``` as being a list of 3 streams.)

Given that none of them are going to affect the other stream, we can safely convert it for multiprocessing as follows:

```python
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
```

### Reduce-Data

Code:

```python
def simple_reducer(key, values):
	return {key: sum(values)}

def reduce_data(sdata, reducer):
	reduced_data = {}
	for key, value in sdata.items():
		reduced_data.update(reducer(key, value))
	return reduced_data
```
Here we notice that while our loop's main purpose is to update ```reduced_data``` dict, we are still running the task in a sequential manner. The reason for this is because ```reduced_data.update``` is not an isolated action, it is an action that relies upon its state. We can not & should not (for now) update shared state objects. Life is much easier this way.

Yet we can apply multiprocessing here. Consider what our ```reducer``` function is doing - It is returning back a dict where we sum up the value list & return a dict. Again, these are but isolated processes which we are being executed sequentially. Hence, our ```reducer``` function is run via multiprocessing. Note we have also updated our ```simple_reducer``` to work with ```reducer```.

```python
def simple_reducer(kv_pair):
    return {kv_pair[0]: sum(kv_pair[1])}

def reduce_data(sdata, reducer):
    pool_size = multiprocessing.cpu_count()*2
    pool = multiprocessing.Pool(processes=pool_size)
    
    reduced_data = dict()
    reduced_list = pool.map(reducer, sdata.items())    
    
    for item in reduced_list:
        reduced_data.update(item)
    
    return reduced_data
```

> DRY PRINCIPLE VIOLATED!
 ___
Before we proceed further, we need to realize that we have written two functions ```reduce_data``` & ```map_data``` with near identical code. In case of ```reduce_data```, even missing out on a few important steps. *Note: It wasn't intentional. But explains why DRY princple is important.* 

Hence we take a slight detour to refactor our code.

### Refactoring for Greator Good!
If we consider our two DRY Principle violating functions side-by-side and try to explain it in generic code it would look pretty much as follows:

```python
def map_pool(func, iter_data):
	pool_size = multiprocessing.cpu_count() * 2
	pool = multiprocessing.Pool(processes=pool_size)
	
	map_pool_data = pool.map(func, iter_data)
	pool.close()
	pool.join()
	
	return map_pool_data
```

Now let's see how our two functions would be rewritten:

```python
def reduce_data(sdata, reducer):
    reduced_data = dict()
    reduced_list = map_pool(reducer, sdata.items())

    for item in reduced_list:
        reduced_data.update(item)

    return reduced_data

def map_data(streams, mapper):
    """
    Accepts list of data streams
    Returns list of - list of tuples
    """
    mapped_data = map_pool(mapper, streams)
    return mapped_data
```

I think that's acceptable.

## Down the Rabbit Hole with Sort-Data & Plex-Sorter

Converting ```sort_data``` & ```simple_sorter``` to Multiprocessing isn't an easy task. In this section, we break the only hard rule we put out near the start, in case you don't remember it:

> (...) is not an isolated action, it is an action that relies upon ~~its~~ *mutable object's* state. We can not & should not ~~(for now)~~ update shared state objects. ~~Life is much easier this way~~ *Unfortunately now it isn't going to be*.

As if testament to how it makes life tough, we will show our code in two forms - One that fails & another that executes correctly. The reason for such failure & success is that when dealing with mutable objects, concurrent/parallel access is unreliable. That's why we have locks and other such constructs in place.

Let's look our two sorter functions again:


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

Let's refactor ```simple_sorter``` to make it easier to identify where we might want to convert it to Multiprocessing.

```python
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
 ```
 The knee jerk reaction would be to suggest that we can convert either or both of the loops into a multiprocessing function(s) and just append into ```sorted_data```. 

And that's the crux of the problem - ```sorted_data``` is a ```dict``` and it is mutable.

Let us try a maiden approach & see how it works out.

### First Try @ MP Sorts
Another way to look at the above refactored ```simple_sorter``` code is to say 
> We have ```n```-lists which are composed of key-value tuples. For each of the ```n```-list, we convert the key-value tuples into a single  hashmap/dict. Then these are all added into a final hashmap/dict which is a shared mutable object.

Since the first two steps can be run in parallel and only the final step requires shared mutable object. We can convert the code accordingly.

Logic for conversion of a list's key-value tuple into a single hashmap/dict:

```python
def plex_sorter(data):
    """
    Accepts a singular simple_mapper output: [(<key1>, 1), (<key2>, 1)...]
    Returns a dict of key & list of tuple's second value: { key1: [1,1,...],...}
    """
    sorted_data = defaultdict(list)

    for data_id, data_val in data:
        sorted_data[data_id].append(data_val)

    return sorted_data
```

The remaining logic can be represented as follows:
```python
def plex_sort_data(mdata, sorter):
    sorted_dicts = map_pool_data(sorter, mdata)
    manager = multiprocessing.Manager()
    keys = set()
    for dct in sorted_dicts:
        for key in dct.keys():
            keys.add(key)
    d = dict()
    for key in keys:
        d[key] = manager.list([0])
    sorted_data = manager.dict(d)
    dict_set = []
    for dct in sorted_dicts:
        dict_set.append([sorted_data, dct])

    map_pool_data(plex_merge_dicts, dict_set)
    return sorted_data

def plex_merge_dicts(dicts):
    plex_dict = dicts[0]
    data_dict = dicts[1]
    keys = data_dict.keys()
    for key in keys:
        value = data_dict[key]
        plex_dict[key].append(value)
```

We use a ```multiprocessing.Manager``` instance to help with our shared state logic. We create a ```Manager.dict``` & ```Manager.list``` which are to be shared. Then we try to run the logic for merging dicts in parallel using ```plex_merge_dicts```.

If we run the above logic with an ```assert``` statement, it fails.
```python
# After Sorting & Reducing, the data in reduced_data is of following form
# reduced_data <- {'RDBMS': 0, 'Hadoop': 0, 'Lisp': 0, 'Java': 0, 'Pascal': 0, 'Prolog': 0}
assert reduced_data == {
        'Java': 4, 'Hadoop': 2, 'RDBMS': 3, 'Prolog': 4, 'Lisp': 2, 'Pascal': 2}
```

There are many reasons the above code doesn't work. Here we will state three that seem most prominent at the moment.
* Python's implementation of Manager requires some extra work. (Resolved in next try).
* ```Pool.Map``` doesn't work for shared state. It expects isolated tasks. (So what should be used?)
* We haven't used any locks. Even though we are using ```multiprocessing.Manager``` we still need to have lock on an object's state, before we make any changes to its state. This ensures that all data access & changes happening in all of the processes is same.

For now let us make a compromise and rewrite our ```sort_data``` for simpler effect.

```python
def sort_data(mdata, sorter):
    """
    Accepts list of - list of tuples
    Returns dict of keys & mapper's tuple value
    """
    pool_size = multiprocessing.cpu_count() * 2
    pool = multiprocessing.Pool(processes=pool_size)
    sorted_dicts = pool.map(sorter, mdata)
    # Notice that we again forgot to close & join our pool.
    
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
```

*Note: We should have used ```defaultdict``` instead of ```{}``` and our if-else logic would have reduced to a single line.*

### Using Process in MultiProcessing.
If we look at ```multiprocessing.Pool```'s documentation, we realize that it is ideal where there is no InterProcess data sharing. Each works in its own isolated world. Hence, we need to use ```multiprocessing.Process```. 

```Process``` is a more powerful & "low" level way to work with multiprocessing than ```Pool``` and when possible we should use ```Pool```. Because it takes care of many things for us.

For our ```plex_sort_data```, we use a bit of old & new and try to achieve following
* Sort our dicts via MP
* Use ```Manager.dict``` for shared dict object
* Use ```Process``` to instantiate our MP logic **for each process instance**
* In ```Process```, we use ```start``` & ```join``` instead of ```close``` & ```join``` individually

Give these steps let's look at how we achive this:
```python
def plex_sort_data(mdata, sorter):

    sorted_dicts = map_pool_data(sorter, mdata)
    manager = multiprocessing.Manager()
    sorted_data = manager.dict()
    keys = set()

    for dct in sorted_dicts:
        for key in dct.keys():
            sorted_data[key] = list()

    processes = []
    for dct in sorted_dicts:
        p = multiprocessing.Process(target=plex_merge_dicts, args=(dct,sorted_data))
        processes.append(p)
        p.start()
    
    for p in processes:
        p.join()
    return sorted_data
```

The ```Manager``` in current Python implementation of Multiprocessing has some glitches which fails to update the shared state. The following code for ```plex_merge_dict``` takes care of it.

```python
def plex_merge_dicts(lock, data_dict, plex_dict):
    keys = data_dict.keys()
    for key in keys:
        # Get shared list
        lst = plex_dict[key]
        value = data_dict[key]
        
        # Update list
        lst+= value
        
        # forces the shared list to be serialized back to manager
        plex_dict[key]=lst
        # Solution: http://stackoverflow.com/a/8644552
```

This pretty much completes the logic for MapReduce with MultiProcessing!


## Not Quite!
What happened when you ran the code? If stars were aligned right, the assertion would pass. But it might fail. If you ran the current implementation more than two or three times, it might fail one of those times. Why you ask?

Because we stated three problems with our previous implementation & resolved only two. To recollect the three points

> 
* *Python's implementation of Manager requires some extra work.* 
* *```Pool.Map``` doesn't work for shared state. It expects isolated tasks.*
* **We haven't used any locks**. Even though we are using ```multiprocessing.Manager``` we still need to have lock on an object's state, before we make any changes to its state. This ensures that all data access & changes happening in all of the processes is same.

### Implementing Lock
The code change for using the ```multiprocessing.Lock``` is quite minimal, thankfully. So we will right away show the code.

```python
def plex_merge_dicts(lock, data_dict, plex_dict):
    with lock:
        keys = data_dict.keys()
        for key in keys:
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
```
Can we be certain that the above code fixes it? What if its just luck? Can we be sure? Well, we need to run it many times to be sure.

### Final Piece of jigsaw

Here we will show our ```start``` function and how we are running the code. We can run our ```start``` for ```n``` number of times and make sure it works. We will run it 50 times. Take special note of easily we are switching between the different components in ```start``` and yet our code works. 

```python
def start(input_files=None, mapper=simple_mapper, sorter=plex_sorter,
          reducer=plex_reducer, output=simple_output):
    input_stream = get_input_stream(input_files)
    mapped_data = map_data(input_stream, mapper)
    sorted_data = plex_sort_data(mapped_data, sorter)
    reduced_data = reduce_data(sorted_data, reducer)
    assert reduced_data == {
        'Java': 4, 'Hadoop': 2, 'RDBMS': 3, 'Prolog': 4, 'Lisp': 2, 'Pascal': 2}


if __name__ == '__main__':
    for i in range(0,50):
        start()
        print('.', end=' ')
```


___

> ![CCANCSA](https://i.creativecommons.org/l/by-nc-sa/4.0/88x31.png)

> This work (refers to text documents only) by Last Ent is licensed under a [Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International License](http://creativecommons.org/licenses/by-nc-sa/4.0/).