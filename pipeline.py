# -*- coding: utf-8 -*-
"""
Created on Mon Oct 31 2022
@name:   Process Objects
@author: Jack Kirby Cook

"""

import os.path
import types
import queue
import inspect
import logging
import pandas as pd
from abc import ABC, abstractmethod

from files.dataframes import DataframeFile
from utilities.dispatchers import kwargsdispatcher

from sync.process import Process
from sync.queues import Queue

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["Producer", "Consumer", "Loader", "Saver"]
__copyright__ = "Copyright 2020, Jack Kirby Cook"
__license__ = ""


LOGGER = logging.getLogger(__name__)


class File(object):
    def __init__(self, *args, repository, **kwargs):
        super().__init__(*args, **kwargs)
        if not os.path.isdir(repository):
            os.mkdir(repository)
        self.__repository = repository

    @property
    def repository(self): return self.__repository
    def file(self, name, ext): return os.path.join(self.repository, ".".join([name, ext]))

    @staticmethod
    def parameters(filename, *args, **kwargs): return {}


class Producer(Process, ABC, daemon=False):
    def __repr__(self): return "{}[{}]".format(self.name, len(self.queue))
    def __len__(self): return len(self.queue)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        name = kwargs.get("name", str(self.__class__.__name__) + "Queue")
        self.__queue = Queue([], name=name)

    def produce(self, query, dataset):
        content = (query, dataset)
        self.queue.put(content)

    def generator(self, *args, **kwargs):
        yield from self.execute(*args, **kwargs)

    def process(self, *args, **kwargs):
        generator = self.generator(*args, **kwargs)
        assert isinstance(generator, types.GeneratorType)
        for putQuery, putDataset in iter(generator):
            self.produce(putQuery, putDataset)
            self.report(putQuery, putDataset)

    def join(self):
        super().join()
        self.queue.join()

    @property
    def queue(self): return self.__queue
    @abstractmethod
    def execute(self, *args, **kwargs): pass
    @staticmethod
    def report(query, dataset): pass


class Consumer(Process, ABC, daemon=True):
    def __repr__(self): return "{}[{}]".format(self.name, len(self.queue))
    def __len__(self): return len(self.queue)

    def __init__(self, *args, source, timeout=60, **kwargs):
        super().__init__(*args, **kwargs)
        name = kwargs.get("name", str(self.__class__.__name__) + "Queue")
        self.__queue = Queue([], name=name)
        self.__source = source
        self.__timeout = int(timeout)

    def consume(self):
        content = self.source.queue.get(timeout=self.timeout)
        query, dataset = content
        return query, dataset

    def produce(self, query, dataset):
        content = (query, dataset)
        self.queue.put(content)

    def generator(self, query, dataset, *args, **kwargs):
        if bool(inspect.isgeneratorfunction(self.execute)):
            yield from self.execute(query, dataset, *args, **kwargs)
        else:
            self.execute(query, dataset, *args, **kwargs)
        return
        yield

    def process(self, *args, **kwargs):
        while bool(self.source) or bool(self.source.queue):
            try:
                getQuery, getDataset = self.consume()
                generator = self.generator(getQuery, getDataset, *args, **kwargs)
                for putQuery, putDataset in iter(generator):
                    self.produce(putQuery, putDataset)
                    self.report(putQuery, putDataset)
                self.source.queue.task_done()
            except queue.Empty:
                continue

    def join(self):
        super().join()
        self.queue.join()

    @property
    def timeout(self): return self.__timeout
    @property
    def source(self): return self.__source
    @property
    def queue(self): return self.__queue
    @abstractmethod
    def execute(self, *args, **kwargs): pass
    @staticmethod
    def report(query, dataset): pass


class Loader(File, Producer):
    def __init__(self, *args, schedule, **kwargs):
        super().__init__(*args, **kwargs)
        self.__schedule = schedule

    def execute(self, *args, **kwargs):
        for query, dataset in self.schedule(*args, **kwargs):
            for filename, filetype in dataset.fields():
                parms = self.parameters(filename, *args, **kwargs)
                file, data = self.load(filename, *args, data=filetype, **parms, **kwargs)
                LOGGER.info("FileLoaded: {}".format(str(file)))
                dataset[filename].append(data)
                yield query, dataset

    @kwargsdispatcher("data")
    def load(self, filename, *args, data, **kwargs): raise TypeError(data.__name__)

    @load.register.value(pd.DataFrame)
    def dataframe(self, filename, *args, index=None, header=None, **kwargs):
        file = self.file(filename, "zip")
        with DataframeFile(*args, file=file, mode="r", **kwargs) as reader:
            dataframe = reader(index=index, header=header)
            return file, dataframe

    @property
    def schedule(self): return self.__schedule
    @schedule.setter
    def schedule(self, schedule): self.__schedule = schedule


class Saver(File, Consumer):
    def execute(self, query, dataset, *args, **kwargs):
        for filename, filedata in iter(dataset):
            parms = self.parameters(filename, *args, **kwargs)
            file = self.save(filename, *args, data=filedata, **parms, **kwargs)
            LOGGER.info("FileSaved: {}".format(str(file)))

    @kwargsdispatcher("data")
    def save(self, filename, *args, data, **kwargs): raise TypeError(type(data).__name__)

    @save.register.type(pd.DataFrame)
    def dataframe(self, filename, *args, data, index=False, header=None, **kwargs):
        file = self.file(filename, "zip")
        with DataframeFile(*args, file=file, mode="a", **kwargs) as writer:
            writer(data, index=index, header=header)
        return file


