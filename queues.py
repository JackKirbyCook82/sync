# -*- coding: utf-8 -*-
"""
Created on Fri Nov 26 2021
@name:   Synchronization Queue Objects
@author: Jack Kirby Cook

"""

import queue

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["Queue"]
__copyright__ = "Copyright 2020, Jack Kirby Cook"
__license__ = ""


class Queue(queue.Queue):
    def __init__(self, *args, timeout=60, **kwargs):
        self.__name = kwargs.get("name", self.__class__.__name__)
        self.__timeout = int(timeout)
        self.__sources = []
        super().__init__()

    def __repr__(self): return "{}[{}]".format(self.name, str(len(self)))
    def __len__(self): return int(self.qsize())

    def __bool__(self):
        sources = any([bool(source) for source in self.sources])
        return not bool(self.empty()) or bool(sources)

    def done(self): super().task_done()
    def get(self): super().get(timeout=self.timeout)
    def put(self, content): super().put(content)

    def register(self, source): self.sources.add(source)
    def unregister(self, source): self.sources.discard(source)

    @property
    def name(self): return self.__name
    @property
    def sources(self): return self.__sources


