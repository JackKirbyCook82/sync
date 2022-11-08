# -*- coding: utf-8 -*-
"""
Created on Mon Oct 31 2022
@name:   Process Objects
@author: Jack Kirby Cook

"""

import types
import os.path
import logging
import queue
import pandas as pd
from abc import ABC, abstractmethod

from files.dataframes import DataframeFile
from utilities.dispatchers import kwargsdispatcher

from sync.status import Status
from sync.process import Process
from sync.queues import Queue

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["Cache", "Producer", "ConsumerProducer", "Consumer", "Loader", "Saver"]
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


class Cache(Queue):
    def __init__(self, *args, timeout=60, **kwargs):
        super().__init__(*args, **kwargs)
        self.__open = Status("Open", True)
        self.__closed = self.__open.divergent("Closed")
        self.__timeout = int(timeout)

    def put(self, query, dataset):
        content = (query, dataset)
        super().put(content, timeout=self.timeout)
        LOGGER.info("Queued: {}".format(repr(self)))
        LOGGER.info(str(query))
        LOGGER.info(str(dataset.results()))

    def get(self):
        content = super().get(timeout=self.timeout)
        query, dataset = content
        return query, dataset

    @property
    def open(self): return self.__open
    @property
    def closed(self): return self.__closed
    @property
    def timeout(self): return self.__timeout


class Producer(Process, ABC, daemon=False):
    def __init__(self, *args, destination, **kwargs):
        super().__init__(*args, **kwargs)
        assert isinstance(destination, Cache)
        self.__destination = destination

    def process(self, *args, **kwargs):
        try:
            generator = self.execute(*args, **kwargs)
            assert isinstance(generator, types.GeneratorType)
            for query, dataset in iter(generator):
                self.destination.put(query, dataset)
        except Exception as error:
            self.destination.closed(True)
            raise error
        self.destination.closed(True)

    @property
    def destination(self): return self.__destination
    @abstractmethod
    def execute(self, *args, **kwargs): pass


class ConsumerProducer(Process, ABC, daemon=False):
    def __init__(self, *args, source, destination, **kwargs):
        super().__init__(*args, **kwargs)
        assert isinstance(source, Cache)
        assert isinstance(destination, Cache)
        self.__source = source
        self.__destination = destination

    def process(self, *args, **kwargs):
        try:
            while bool(self.source.open):
                try:
                    query, dataset = self.source.get()
                except queue.Empty:
                    continue
                generator = self.execute(query, dataset, *args, **kwargs)
                assert isinstance(generator, types.GeneratorType)
                for query, dataset in iter(generator):
                    self.destination.put(query, dataset)
        except Exception as error:
            self.destination.closed(True)
            raise error
        self.destination.closed(True)

    @property
    def source(self): return self.__source
    @property
    def destination(self): return self.__destination
    @abstractmethod
    def execute(self, *args, **kwargs): pass


class Consumer(Process, ABC, daemon=True):
    def __init__(self, *args, source, **kwargs):
        super().__init__(*args, **kwargs)
        assert isinstance(source, Cache)
        self.__source = source

    def process(self, *args, **kwargs):
        while bool(self.source.open):
            try:
                query, dataset = self.source.get()
            except queue.Empty:
                continue
            self.execute(query, dataset, *args, **kwargs)

    @property
    def source(self): return self.__source
    @abstractmethod
    def execute(self, *args, **kwargs): pass


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


