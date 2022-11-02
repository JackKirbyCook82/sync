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

from files.dataframes import DataframeFile
from utilities.dispatchers import kwargsdispatcher

from sync.thread import Thread

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["User", "Loader", "Saver", "Downloader"]
__copyright__ = "Copyright 2020, Jack Kirby Cook"
__license__ = ""


LOGGER = logging.getLogger(__name__)


class File(object):
    def __init__(self, *args, repository, **kwargs):
        super().__init__(*args, **kwargs)
        if not os.path.isdir(repository):
            os.mkdir(repository)
        self.__repository = repository
        self.__index = self.__class__.__index__

    @property
    def repository(self): return self.__repository
    def file(self, name, ext): return os.path.join(self.repository, ".".join([name, ext]))

    @staticmethod
    def parameters(filename, *args, **kwargs): return {}


class Process(Thread, ABC):
    def __init__(self, *args, queue, **kwargs):
        super().__init__(*args, **kwargs)
        self.__results = {}
        self.__queue = queue

    def report(self, queueable):
        results = {key: value for key, value in self.results.items()}
        results[queueable.query] = queueable.dataset.results() if queueable.query not in results else results[queueable.query] + queueable.dataset.results()
        self.results = results

    def display(self):
        for query, results in self.results.items():
            LOGGER.info(str(query))
            LOGGER.info(str(results))

    @property
    def queue(self): return self.__queue
    @property
    def results(self): return self.__results
    @results.setter
    def results(self, results): self.__results = results

    @abstractmethod
    def execute(self, *args, **kwargs): pass


class Producer(Process, ABC, daemon=False):
    def process(self, *args, **kwargs):
        for query, dataset in self.execute(*args, **kwargs):
            queueable = Queueable(query, dataset)
            self.queue.put(queueable)
            LOGGER.info("Produced: {}".format(repr(self)))
            LOGGER.info(str(queueable.query))
            LOGGER.info(str(queueable.dataset))
            self.report(queueable)
        self.display()


class Consumer(Process, ABC, daemon=True):
    def process(self, *args, **kwargs):
        while True:
            queueable = self.queue.get()
            assert isinstance(queueable, Queueable)
            self.execute(queueable, *args, **kwargs)
            LOGGER.info("Consumed: {}".format(repr(self)))
            LOGGER.info(str(queueable.query))
            LOGGER.info(str(queueable.dataset))
            self.report(queueable)
        self.display()


class Downloader(Producer, ABC): pass
class User(Consumer, ABC): pass


class Loader(File, Producer):
    def __init__(self, *args, schedule, **kwargs):
        super().__init__(*args, **kwargs)
        self.__schedule = schedule

    def execute(self, *args, **kwargs):
        for query, dataset in self.schedule(*args, **kwargs):
            for filename, filetype in dataset.fields():
                parms = self.parameters(filename, *args, **kwargs)
                data = self.load(filename, *args, data=filetype, **parms, **kwargs)
                dataset[filename].append(data)
                queueable = Queueable(query, dataset)
                yield queueable

    @kwargsdispatcher("data")
    def load(self, filename, *args, data, **kwargs): raise TypeError(data.__name__)

    @load.register.value(pd.DataFrame)
    def dataframe(self, filename, *args, index=None, header=None, **kwargs):
        file = self.file(filename, "zip")
        with DataframeFile(*args, file=file, mode="r", **kwargs) as reader:
            return reader(index=index, header=header)

    @property
    def schedule(self): return self.__schedule
    @schedule.setter
    def schedule(self, schedule): self.__schedule = schedule


class Saver(File, Consumer):
    def execute(self, queueable, *args, **kwargs):
        assert isinstance(queueable, Queueable)
        for filename, filedata in iter(queueable.dataset):
            parms = self.parameters(filename, *args, **kwargs)
            self.save(filename, *args, data=filedata, **parms, **kwargs)

    @kwargsdispatcher("data")
    def save(self, filename, *args, data, **kwargs): raise TypeError(type(data).__name__)

    @save.register.type(pd.DataFrame)
    def dataframe(self, filename, *args, data, index=False, header=None, **kwargs):
        file = self.file(filename, "zip")
        with DataframeFile(*args, file=file, mode="a", **kwargs) as writer:
            writer(data, index=index, header=header)



