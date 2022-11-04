# -*- coding: utf-8 -*-
"""
Created on Mon Oct 31 2022
@name:   Process Objects
@author: Jack Kirby Cook

"""

import os.path
import logging
import pandas as pd
from abc import ABC, abstractmethod
from collections import namedtuple as ntuple

from files.dataframes import DataframeFile
from utilities.dispatchers import kwargsdispatcher

import sync.thread
import sync.queue

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["Queue", "Producer", "Pipeline", "Consumer", "Loader", "Saver"]
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


class Queueable(ntuple("Queueable", "query dataset")):
    pass


class Queue(sync.queue.Queue):
    def put(self, query, dataset):
        queueable = Queueable(query, dataset)
        super().put(queueable)
        LOGGER.info("Queued: {}".format(repr(self)))
        LOGGER.info(str(query))
        LOGGER.info(str(dataset.results()))

    def get(self):
        queueable = super().get()
        assert isinstance(queueable, Queueable)
        query, dataset = queueable
        return query, dataset


class Process(sync.thread.Thread, ABC):
    @abstractmethod
    def process(self, *args, **kwargs): pass
    @abstractmethod
    def execute(self, *args, **kwargs): pass


class Producer(Process, ABC, daemon=False):
    def __init__(self, *args, destination, **kwargs):
        super().__init__(*args, **kwargs)
        assert isinstance(destination, Queue)
        self.__destination = destination

    def process(self, *args, **kwargs):
        for query, dataset in self.execute(*args, **kwargs):
            self.destination.put(query, dataset)

    @property
    def destination(self): return self.__destination


class Pipeline(Process, ABC, daemon=False):
    def __init__(self, *args, source, destination, **kwargs):
        super().__init__(*args, **kwargs)
        assert isinstance(source, Queue)
        assert isinstance(destination, Queue)
        self.__source = source
        self.__destination = destination

    def process(self, *args, **kwargs):
        while True:
            inQuery, inDataset = self.source.get()
            for outQuery, outDataset in self.execute(inQuery, inDataset, *args, **kwargs):
                self.destination.put(outQuery, outDataset)

    @property
    def source(self): return self.__source
    @property
    def destination(self): return self.__destination


class Consumer(Process, ABC, daemon=True):
    def __init__(self, *args, source, **kwargs):
        super().__init__(*args, **kwargs)
        assert isinstance(source, Queue)
        self.__source = source

    def process(self, *args, **kwargs):
        while True:
            query, dataset = self.source.get()
            self.execute(query, dataset, *args, **kwargs)

    @property
    def source(self): return self.__source


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


