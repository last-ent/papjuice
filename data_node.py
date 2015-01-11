import abc
import random


class AbstractNodeClass(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def get_data(self):
        raise NotImplementedError

    @abc.abstractmethod
    def get_health(self):
        raise NotImplementedError


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


class DataStream(object):

    def __init__(self, data):
        self.parallel_nodes = []
        self.index = 0
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

    def __get_node_data(self, index):
        for data_node in self.parallel_nodes:
            if data_node.get_health():
                return data_node.get_data(index)
        raise Exception('PCLOADLETTER')
