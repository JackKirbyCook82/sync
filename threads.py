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

from sync.status import Status

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["Thread"]
__copyright__ = "Copyright 2020, Jack Kirby Cook"
__license__ = ""


LOGGER = logging.getLogger(__name__)


class Thread(threading.Thread, ABC):
    def __repr__(self): return "{}".format(self.name)
    def __bool__(self): return self.is_alive()

    def __init_subclass__(cls, *args, daemon=False, **kwargs):
        failures = list(getattr(cls, "__failures__", []))
        update = [kwargs.get("failure", None)] + list(kwargs.get("failures", []))
        failures = filter(None, failures + update)
        assert all([issubclass(exception, BaseException) for exception in failures])
        cls.__failures__ = tuple(failures)
        cls.__daemon__ = daemon

    def __init__(self, *args, **kwargs):
        name = kwargs.get("name", self.__class__.__name__)
        threading.Thread.__init__(self, name=str(name), daemon=self.__class__.__daemon__)
        self.__arguments = list()
        self.__parameters = dict()
        self.__running = Status("Running", False)
        self.__idle = self.__running.divergent("Idle")
        self.__failures = tuple([exception for exception in self.__class__.__failures__])
        self.__failure = None
        self.__error = None

    def __call__(self, *arguments, **parameters):
        self.__arguments.extend(list(arguments))
        self.__parameters.update(dict(parameters))
        return self

    def start(self):
        LOGGER.info("Started: {}".format(repr(self)))
        super().start()

    def join(self):
        super().join()
        LOGGER.info("Stopped: {}".format(repr(self)))

    def run(self):
        try:
            self.process(*self.__arguments, **self.__parameters)
        except self.__failures as failure:
            LOGGER.warning("Failure: {}|{}".format(repr(self), failure.__class__.__name__))
            self.__failure = failure
        except BaseException as error:
            LOGGER.error("Error: {}|{}".format(repr(self), error.__class__.__name__))
            error_type, error_value, error_traceback = sys.exc_info()
            traceback.print_exception(error_type, error_value, error_traceback)
            self.__error = error
        else:
            LOGGER.info("Completed: {}".format(repr(self)))

    @property
    def failure(self): return self.__failure
    @property
    def error(self): return self.__error

    @staticmethod
    def sleep(seconds): sleeper(seconds)
    @abstractmethod
    def process(self, *args, **kwargs): pass


