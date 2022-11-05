# -*- coding: utf-8 -*-
"""
Created on Fri Nov 26 2021
@name:   Synchronization Process Objects
@author: Jack Kirby Cook

"""

import sys
import logging
import threading
import traceback
from abc import ABC, abstractmethod
from time import sleep as sleeper
from collections import namedtuple as ntuple

from sync.status import Status

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["Process"]
__copyright__ = "Copyright 2020, Jack Kirby Cook"
__license__ = ""


LOGGER = logging.getLogger(__name__)


_aslist = lambda items: list(items) if isinstance(items, (tuple, list, set)) else [items]
_astuple = lambda items: tuple(items) if isinstance(items, (tuple, list, set)) else (items,)
_filter = lambda items, by: [item for item in _aslist(items) if item is not by]


class ControlProcess(Exception):
    def __str__(self): return "{}|{}".format(self.__class__.__name__, repr(self.args[0]))
    def __int__(self): return int(self.args[1])


class ErrorTuple(ntuple("Error", "type value traceback")): pass
class TerminateProcess(ControlProcess): pass


class Process(threading.Thread, ABC):
    def __init_subclass__(cls, *args, failure=None, failures=[], daemon=False, **kwargs):
        assert all([issubclass(exception, BaseException) for exception in _filter(_aslist(failure) + _aslist(failures), None)])
        assert isinstance(daemon, bool)
        cls.failures = list(getattr(cls, "failures", []))
        cls.failures = tuple(_filter(cls.failures + _aslist(failure) + _aslist(failures), None))
        cls.daemon = daemon

    def __init__(self, *args, name=None, **kwargs):
        assert isinstance(name, str)
        name = name if name is not None else self.__class__.__name__
        daemon = self.__class__.daemon
        threading.Thread.__init__(self, name=name, daemon=daemon)
        self.__arguments = list()
        self.__parameters = dict()
        self.__alive = Status("Alive", False)
        self.__dead = self.__alive.divergent("Dead")
        self.__running = Status("Running", False)
        self.__idle = self.__running.divergent("Idle")
        self.__failures = tuple([exception for exception in self.__class__.failures])
        self.__failure = None
        self.__error = None
        self.__traceback = None

    def __repr__(self): return "{}".format(self.name)
    def __bool__(self): return self.is_alive()

    def __call__(self, *arguments, **parameters):
        self.__arguments.extend(list(arguments))
        self.__parameters.update(dict(parameters))
        return self

    def run(self):
        self.alive(True)
        try:
            LOGGER.info("Started: {}".format(repr(self)))
            self.running(True)
            self.process(*self.__arguments, **self.__parameters)
        except TerminateProcess:
            LOGGER.warning("Terminated: {}".format(repr(self)))
        except self.__failures as failure:
            LOGGER.warning("Failure: {}|{}".format(repr(self), failure.__class__.__name__))
            self.__failure = failure
        except BaseException as error:
            LOGGER.error("Error: {}|{}".format(repr(self), error.__class__.__name__))
            error_type, error_value, error_traceback = sys.exc_info()
            traceback.print_exception(error_type, error_value, error_traceback)
            self.__error = ErrorTuple(error_type, error_value, error_traceback)
        else:
            LOGGER.info("Completed: {}".format(repr(self)))
        finally:
            LOGGER.info("Stopping: {}".format(repr(self)))
            self.idle(True)
        self.dead(True)

    @staticmethod
    def sleep(seconds): sleeper(seconds)
    def terminate(self): raise TerminateProcess(self)

    @property
    def failure(self): return self.__failure
    @property
    def error(self): return self.__error

    @property
    def alive(self): return self.__alive
    @property
    def dead(self): return self.__dead

    @property
    def running(self): return self.__running
    @property
    def idle(self): return self.__idle

    @abstractmethod
    def process(self, *args, **kwargs): pass


