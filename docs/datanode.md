## Data & Fault Tolerance
Now that we have covered most of the required concepts, let's have a look at one last one. We know that MapReduce systems are supposed to be fault tolerant and not have a list of strings as in our implementation. Now that we have most of the system in place. We can easily abstract away the Input Streams to provide identical functionality.

We will be using two main constructs - ```DataStream```& ```DataNodes``` to implement this. If we were to draw parallels between the current ```input_stream = ['str1', 'str2', 'str3']``` & the new one. 

It would be as follows:

	String : List :: DataNode : DataStream
To put it in words - A ```DataStream``` consists of a stream of ```DataNodes```.

In a real MapReduce system, our strings within a list are analogous **Blocks**. If we have a big file with over Gigabytes of data, then a ```Block``` corresponds to a small part of it which is stored as a single unit. Hadoop/MapReduce systems store this ```Block``` on multiple servers so that if one fails or is under heavy load, it can access data from another point.

In order to better understand this, we will implement classes ```DataNode``` & ```DataStream```. The official names as per Hadoop/MapReduce will be different but we can bypass them for now.

> Note: While we provide logic for ```DataNode``` & ```DataStream```, please keep in mind that both of them are but an intermix of various functionalities found for data maintenance.

### DataNode
As previously stated, a ```DataNode``` consists of the smallest divisible block/chunk of data as per MapReduce system. In our case, this will a single String. But we cannot simply return back a String, for it will not be "fault tolerant". Hence we create our ```DataNode``` class.

A DataNode class has to follow a contract defined as follows:
* Accept Data for storing purposes
* Return Data Block 
* Relay Status of Node

We will first write an abstract class to define the contract:
```python
import abc

class AbstractNodeClass(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def get_data(self):
        raise NotImplementedError

    @abc.abstractmethod
    def get_health(self):
        raise NotImplementedError
```
*Note: While Abstract Base Class (abc) shouldn't require us to raise ```NotImplementedError```, we are doing here because our code isn't raising an error when an object is being instantiated without it being implemented in child class.*

In our implementation of ```DataNode```, apart from data steam, we will  also be providing a number to signify which of the three data nodes is being used. As we will see further, this allows us to simulate node failure in ```DataNode.get_health``` and check if our system works as expected in such cases. 

Now let us have a look at our implementation of DataNode
```python
class DataNode(AbstractNodeClass):

    def __init__(self, data, node):
        self.data = data
        self.node = node

    def get_data(self, iter_val):
        return self.data[iter_val]

    def get_health(self):
        if 0 <= self.node < 2:
            health = [True, False][random.randint(0, 1)]
            return health
        else:
            return True
```

Based on above discussion, all the steps should be fairly obvious. The only confusion might arise with why we are simulating failure for only first two nodes and not the third. The reason is that we want to simulate fault tolerance and not failure.

### DataStream
A List in Python is an iterator and so we need to implement our ```DataStream``` as an iterator that returns each "block" of data. Otherwise the logic developed in the rest of the application will fail.

> Iterator vs. Generator
> ---
> * An Iterator is a class and needs the developer to implement ```__iter__``` and ```__next___``` (in case of python3) | ```next``` (in case of python2). We will need to raise ```StopIteration``` instance to end iteration.

> * A Generator is a function which ```yield```s required output and stops once all ```yield``` statements are executed.

Next let us plan what or how we intend our ```DataStream``` to achieve.
* Take Data and create `DataNodes`
* Iterate over data by calling on ```DataNode```.
* Incase of failure at one Node, call on next node.

Code for this would be as follows:
```python
class DataStream(object):

    def __init__(self, data):
        self.index = 0
        self.parallel_nodes = []
        self.data_size = len(data)

        for i in range(0, 3):
            self.parallel_nodes.append(DataNode(data, i))

    def __iter__(self):
        return self

    def __next__(self):
        return self.next()

    def next(self):
        if self.index < self.data_size:
            data = self.__get_node_data(self.index)
            self.index += 1
            return data
        else:
            raise StopIteration()
```

This achieves first two points but let us deliberate a bit before looking at code for third step and a better look at `next`.

Consider when we ask for data from a node and it fails to deliver, we move onto next node. When we call again and first node responds, how are we going to ensure that the data it delivers is the one at correct index? For this reason our method `__get_node_data` takes an argument and it is being incremented at `DataStream` and not `DataNode`.

Now consider `__get_node_data` - It takes data index & it needs to check each of the Data Nodes for availability & request for required data.

With this in mind, let us look at its implementation:
```python
class DataStream(object):
    ...
    def __get_node_data(self, index):
        for data_node in self.parallel_nodes:
            if data_node.get_health():
                return data_node.get_data(index)
        raise Exception('PCLOADLETTER') 
        # Should never be raised.
```

## Updating Input-Streams
Given our design so far, we should be able to easily insert our new Input Stream to work with rest of the code. Let's have a look.
```python
def get_input_stream(inputs):
    if not inputs:
        return get_sample_streams()
    elif isinstance(inputs, type) and issubclass(inputs, DataStream):
        returnable = list()
        for data_stream in get_sample_streams():
            returnable.append(DataStream(data_stream))
        return returnable
    else:
        raise Exception('Input situation not addressed.')

def start(input_files=DataStream, mapper=simple_mapper,
		  sorter=plex_sorter, reducer=plex_reducer,
		  output=simple_output):
    ...
```

The rest of the code works as expected. Hence we conclude 

	PapJuice - Python through Python Goggles.

> What Next?
> ---
> I might further explore implementing MapReduce patterns using `PapJuice` just for the heck of it. In the mean time, let me know what you think about this article.

___

> ![CCANCSA](https://i.creativecommons.org/l/by-nc-sa/4.0/88x31.png)

> This work (refers to text documents only) by Last Ent is licensed under a [Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International License](http://creativecommons.org/licenses/by-nc-sa/4.0/).
