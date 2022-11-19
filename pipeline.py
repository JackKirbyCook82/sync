# -*- coding: utf-8 -*-
"""
Created on Mon Oct 31 2022
@name:   Process Objects
@author: Jack Kirby Cook

"""

import os.path
import types
import queue
import logging
import pandas as pd
from abc import ABC, abstractmethod

from files.dataframes import DataframeFile
from utilities.dispatchers import kwargsdispatcher

from sync.process import Process
from sync.queues import Queue

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["Producer", "Pipeline", "Consumer", "Cache", "Loader", "Saver"]
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
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__feeds = []

    def __bool__(self):
        feeds = any([bool(feed) for feed in self.feeds])
        return super().__bool__() or bool(feeds)

    @property
    def feeds(self): return self.__feeds
    def register(self, feed): self.__feeds.append(feed)


class Producer(Process, ABC, daemon=False):
    def __init__(self, *args, destination, **kwargs):
        super().__init__(*args, **kwargs)
        assert isinstance(destination, Queue)
        self.__destination = destination
        destination.register(self)

    def process(self, *args, **kwargs):
        generator = self.execute(*args, **kwargs)
        assert isinstance(generator, types.GeneratorType)
        for content in iter(generator):
            self.destination.put(content)
            self.report(content)

    def report(self, content):
        pass

    def join(self):
        super().join()
        self.destination.join()

    @property
    def destination(self): return self.__destination
    @abstractmethod
    def execute(self, *args, **kwargs): pass


class Consumer(Process, ABC, daemon=True):
    def __init__(self, *args, source, timeout=60, **kwargs):
        super().__init__(*args, **kwargs)
        assert isinstance(source, Queue)
        self.__source = source
        self.__timeout = int(timeout)

    def process(self, *args, **kwargs):
        while bool(self.source):
            try:
                content = self.source.get(timeout=self.timeout)
                self.execute(content, *args, **kwargs)
                self.source.task_done()
            except queue.Empty:
                continue

    def join(self):
        super().join()

    @property
    def timeout(self): return self.__timeout
    @property
    def source(self): return self.__source
    @abstractmethod
    def execute(self, *args, **kwargs): pass


class Pipeline(Process, ABC, daemon=True):
    def __init__(self, *args, source, destination, timeout=60, **kwargs):
        super().__init__(*args, **kwargs)
        assert isinstance(source, Queue)
        assert isinstance(destination, Queue)
        self.__source = source
        self.__destination = destination
        self.__timeout = int(timeout)
        destination.register(self)

    def process(self, *args, **kwargs):
        while bool(self.source):
            try:
                getContent = self.queue.get(timeout=self.timeout)
                generator = self.execute(getContent, *args, **kwargs)
                assert isinstance(generator, types.GeneratorType)
                for putContent in iter(generator):
                    self.destination.put(putContent)
                    self.report(putContent)
                self.source.task_done()
            except queue.Empty:
                continue

    def report(self, content):
        pass

    def join(self):
        super().join()
        self.destination.join()

    @property
    def source(self): return self.__source
    @property
    def destination(self): return self.__destination
    @property
    def timeout(self): return self.__timeout
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
    def execute(self, content, *args, **kwargs):
        (query, dataset) = content
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


