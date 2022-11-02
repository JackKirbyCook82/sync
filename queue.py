# -*- coding: utf-8 -*-
"""
Created on Fri Nov 26 2021
@name:   Synchronization Objects
@author: Jack Kirby Cook

"""

import queue
import logging
import threading

from files.csvs import CSVFile
from utilities.observer import Event, Publisher, Subscriber

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["Queue", "Task", "Stack"]
__copyright__ = "Copyright 2020, Jack Kirby Cook"
__license__ = ""


LOGGER = logging.getLogger(__name__)


_aslist = lambda items: list(items) if isinstance(items, (tuple, list, set)) else [items]
_astuple = lambda items: tuple(items) if isinstance(items, (tuple, list, set)) else (items,)
_filter = lambda items, by: [item for item in _aslist(items) if item is not by]


class Queue(queue.Queue):
    def __init__(self, *args, items=[], name=None, **kwargs):
        assert isinstance(items, list)
        queue.Queue.__init__(self)
        self.__name = name if name is not None else self.__class__.__name__
        for item in items:
            self.put(item)

    def __repr__(self): return "{}|{}".format(self.name, str(len(self)))
    def __bool__(self): return not bool(self.empty())
    def __len__(self): return int(self.qsize())
    def __iter__(self): return self

    def __getitem__(self, size):
        assert isinstance(size, int)
        with self.mutex:
            instance = Queue([], name=self.name)
            for _ in range(min([int(size), len(self)])):
                instance.put(self.get())
            return instance

    def __next__(self):
        if not bool(self):
            raise StopIteration
        return self.get()

    @property
    def name(self): return self.__name


class Abandon(Event): pass
class Success(Event): pass
class Failure(Event): pass
class Error(Event): pass


class Task(Publisher):
    def __init__(self, *args, name=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.__name = name if name is not None else self.__class__.__name__
        self.__defaults = dict(abandon=Abandon, success=Success, failure=False, error=Error)
        self.__default = None
        self.__result = None

    def __repr__(self): return "{}".format(self.name)
    def __call__(self, default): self.default = self.__defaults[default]
    def __enter__(self): return self

    def __exit__(self, error_type, error_value, error_traceback):
        if error_type is not None and self.result is None:
            self.publish(Error, self)
        elif error_type is not None and self.result is not None:
            assert self.result is Error
        elif error_type is None and self.result is None:
            assert self.default is not None
            self.publish(self.default, self)
        elif error_type is None and self.result is not None:
            assert self.result is not Error

    @property
    def name(self): return self.__name
    @property
    def defaults(self): return self.__defaults
    @property
    def default(self): return self.__default
    @default.setter
    def default(self, default): self.__default = default
    @property
    def result(self): return self.__result
    @result.setter
    def result(self, result): self.__result = result

    def abandon(self): self.publish(Abandon, self)
    def success(self): self.publish(Success, self)
    def failure(self): self.publish(Failure, self)
    def error(self): self.publish(Error, self)


class Stack(Subscriber):
    def __init__(self, *args, name=None, file, **kwargs):
        super().__init__(*args, events={Success: self.success, Failure: self.failure, Abandon: self.abandon}, **kwargs)
        self.__name = name if name is not None else self.__class__.__name__
        self.__agenda = Queue([], name="agenda")
        self.__success = Queue([], name="success")
        self.__failure = Queue([], name="failure")
        self.__file = file
        self.__mutex = threading.Lock()
        try:
            taskitems = args[0]
            assert isinstance(taskitems, list)
            assert all([isinstance(taskitem, Task) for taskitem in _aslist(taskitems)])
            self.scheduler(taskitems)
        except IndexError:
            pass

    def __repr__(self): return "{}[{}]".format(self.name, str(len(self)))
    def __bool__(self): return bool(self.__agenda)
    def __len__(self): return len(self.__agenda)

    def __call__(self, taskitems): return self.scheduler(taskitems)
    def __getitem__(self, size): return self.export(size)
    def __next__(self): return self.checkout()
    def __iter__(self): return self

    def __enter__(self): return self
    def __exit__(self, error_type, error_value, error_traceback): self.close()

    @property
    def name(self): return self.__name
    @property
    def file(self): return self.__file
    @property
    def mutex(self): return self.__mutex

    def scheduler(self, taskitems):
        assert isinstance(taskitems, list)
        assert all([isinstance(taskitem, Task) for taskitem in _aslist(taskitems)])
        with self.mutex:
            LOGGER.info("Scheduling: {}".format(self.name))
            for taskitem in _aslist(taskitems):
                taskitem.register(self)
                self.__agenda.put(taskitem)
            LOGGER.info("Scheduled: {}".format(repr(self)))
        return self

    def schedule(self, taskitem):
        assert isinstance(taskitem, Task)
        with self.mutex:
            LOGGER.info("Schedule: {}".format(self.name))
            taskitem.register(self)
            self.__agenda.put(taskitem)
            LOGGER.info("Scheduled: {}".format(repr(self)))

    def abandon(self, taskitem):
        assert isinstance(taskitem, Task)
        LOGGER.info("Abandon: {}".format(repr(taskitem)))
        with self.mutex:
            self.__agenda.put(taskitem)

    def success(self, taskitem):
        assert isinstance(taskitem, Task)
        LOGGER.info("Success: {}".format(repr(taskitem)))
        with self.mutex:
            self.__success.put(taskitem)

    def failure(self, taskitem):
        assert isinstance(taskitem, Task)
        LOGGER.info("Failure: {}".format(repr(taskitem)))
        with self.mutex:
            self.__failure.put(taskitem)

    def error(self, taskitem):
        assert isinstance(taskitem, Task)
        LOGGER.info("Error: {}".format(repr(taskitem)))
        with self.mutex:
            self.__abandon.put(taskitem)

    def checkout(self):
        if not bool(self):
            raise StopIteration
        with self.mutex:
            LOGGER.info("CheckOut: {}".format(repr(self)))
            taskitem = self.__agenda.get()
            LOGGER.info("CheckedOut: {}".format(repr(taskitem)))
            return taskitem

    def export(self, size):
        assert isinstance(size, int)
        if not bool(self):
            instance = self.__class__([], name=self.name, file=self.file)
            return instance
        with self.mutex:
            LOGGER.info("Exporting: {}".format(repr(self)))
            taskitems = []
            for _ in range(min([int(size), len(self)])):
                taskitem = self.__agenda.get()
                taskitem.unregister(self)
                taskitems.append(taskitem)
            instance = self.__class__(taskitems, name=self.name, file=self.file)
            return instance

    def close(self):
        if not bool(self.file):
            return
        with self.mutex:
            LOGGER.info("Reporting[{:.0f}]: {}".format(len(self.__success), repr(self)))
            taskitems = []
            for taskitem in iter(self.__success):
                taskitem.unregister(self)
                taskitems.append(taskitem.todict())
            self.report(self.file, taskitems)

    @staticmethod
    def report(file, taskitems):
        with CSVFile(file=file, mode="a", index=False, header=True, parsers={}, parser=None) as appender:
            for taskitem in taskitems:
                appender(taskitem.todict())

