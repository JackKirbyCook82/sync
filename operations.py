# -*- coding: utf-8 -*-
"""
Created on Mon Oct 31 2022
@name:   Process Objects
@author: Jack Kirby Cook

"""

import types
import queue
import logging
from abc import ABC, abstractmethod

from sync.threads import Thread
from sync.queues import Queue

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["Producer", "Consumer", "Pipeline", "Process"]
__copyright__ = "Copyright 2020, Jack Kirby Cook"
__license__ = ""


LOGGER = logging.getLogger(__name__)


class Producer(Thread, ABC, daemon=False):
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

    def join(self):
        super().join()
        self.destination.join()

    @property
    def destination(self): return self.__destination
    @abstractmethod
    def execute(self, *args, **kwargs): pass


class Consumer(Thread, ABC, daemon=True):
    def __init__(self, *args, source, **kwargs):
        super().__init__(*args, **kwargs)
        assert isinstance(source, Queue)
        self.__source = source

    def process(self, *args, **kwargs):
        while bool(self.source):
            try:
                content = self.source.get()
                self.execute(content, *args, **kwargs)
                self.source.task_done()
            except queue.Empty:
                continue

    def join(self):
        super().join()

    @property
    def source(self): return self.__source
    @abstractmethod
    def execute(self, *args, **kwargs): pass


class Operation(Thread, ABC, daemon=True):
    def __init__(self, *args, source, destination, **kwargs):
        super().__init__(*args, **kwargs)
        assert isinstance(source, Queue)
        assert isinstance(destination, Queue)
        self.__source = source
        self.__destination = destination
        destination.register(self)

    def join(self):
        super().join()
        self.destination.join()

    @property
    def source(self): return self.__source
    @property
    def destination(self): return self.__destination


class Pipeline(Operation, ABC):
    def process(self, *args, **kwargs):
        while bool(self.source):
            try:
                getContent = self.source.get()
                generator = self.execute(getContent, *args, **kwargs)
                assert isinstance(generator, types.GeneratorType)
                for putContent in iter(generator):
                    self.destination.put(putContent)
                self.source.done()
            except queue.Empty:
                continue

    @abstractmethod
    def execute(self, content, *args, **kwargs): pass


class Process(Operation, ABC):
    def process(self, *args, **kwargs):
        while bool(self.source):
            try:
                getContent = self.source.get()
                self.consume(getContent, *args, **kwargs)
                self.sleep(self.timeout)
                self.source.done()
                putContent = self.produce(*args, **kwargs)
                self.destination.put(putContent)
            except queue.Empty:
                continue

    @abstractmethod
    def consume(self, content, *args, **kwargs): pass
    @abstractmethod
    def produce(self, *args, **kwargs): pass



