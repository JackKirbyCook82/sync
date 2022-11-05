# -*- coding: utf-8 -*-
"""
Created on Fri Nov 26 2021
@name:   Synchronization Status Objects
@author: Jack Kirby Cook

"""

import threading

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["Status"]
__copyright__ = "Copyright 2020, Jack Kirby Cook"
__license__ = ""


class Switch(object):
    def __init__(self, state):
        assert isinstance(state, bool)
        self.__state = state
        self.__on = threading.Event()
        self.__off = threading.Event()
        self.__lock = threading.Lock()
        self.set(state)

    def __repr__(self): return "{}|{}".format(self.__class__.__name__, str(bool(self)))
    def __bool__(self): return bool(self.__state)
    def __call__(self, state): self.set(state)

    @property
    def state(self): return self.__state

    def on(self): self.set(True)
    def off(self): self.set(False)
    def flip(self): self.set(not bool(self.state))

    def set(self, state):
        assert isinstance(state, bool)
        with self.__lock:
            self.__state = state
            if bool(state):
                self.__on.set()
                self.__off.clear()
            else:
                self.__on.clear()
                self.__off.set()

    def wait(self, state, timeout=None):
        assert isinstance(state, bool)
        if bool(state):
            return self.__on.wait(timeout=timeout)
        else:
            return self.__off.wait(timeout=timeout)


class StatusMeta(type):
    def __call__(cls, name, *args, **kwargs):
        try:
            instance = cls.direct(name, *args, **kwargs)
        except TypeError:
            instance = cls.indirect(name, *args, **kwargs)
        return instance

    def direct(cls, name, *args, switch, position=True, **kwargs):
        assert isinstance(switch, Switch) and isinstance(position, bool)
        return super(StatusMeta, cls).__call__(name, switch=switch, position=position)

    def indirect(cls, name, state, *args, position=True, **kwargs):
        assert isinstance(state, bool)
        return super(StatusMeta, cls).__call__(name, switch=Switch(state), position=position)


class Status(object, metaclass=StatusMeta):
    def __init__(self, name, switch, position):
        assert isinstance(switch, Switch)
        assert isinstance(position, bool)
        self.__name = name
        self.__switch = switch
        self.__position = position

    def __repr__(self): return "{}|{}|{}".format(self.__class__.__name__, self.__name.title(), str(bool(self)))
    def __bool__(self): return bool(self.__position) is bool(self.__switch)
    def __call__(self, state): self.__switch(bool(self.__position) is bool(state))

    def wait(self, timeout=None): return self.__switch.wait(self.__position, timeout=timeout)
    def convergent(self, name): return self.__class__(name, switch=self.__switch, position=self.__position)
    def divergent(self, name): return self.__class__(name, switch=self.__switch, position=not self.__position)

