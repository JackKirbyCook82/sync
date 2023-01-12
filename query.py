# -*- coding: utf-8 -*-
"""
Created on Mon Oct 31 2022
@name:   Fields Objects
@author: Jack Kirby Cook

"""

from collections import OrderedDict as ODict

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["Query", "Dataset"]
__copyright__ = "Copyright 2020, Jack Kirby Cook"
__license__ = ""


class Query(tuple):
    def __init_subclass__(cls, *args, fields=[], **kwargs):
        cls.fields = tuple([*getattr(cls, "fields", tuple()), *fields])

    def __new__(cls, contents, *args, **kwargs):
        assert isinstance(contents, dict)
        assert set(list(contents.keys())) == set(list(cls.fields))
        return super().__new__(cls, [contents[field] for field in cls.fields])

    def __init__(self, *args, name=None, **kwargs): self.__name = name if name is not None else self.__class__.__name__
    def __repr__(self): return "{}[{}]".format(self.name, ", ".join(["=".join([key, str(value) if value is not None else ""]) for key, value in self.items()]))
    def __str__(self): return "{}: {}".format(self.name, ", ".join(["=".join([key, str(value) if value is not None else ""]) for key, value in self.items()]))
    def __hash__(self): return hash(frozenset(self.items()))
    def __eq__(self, other): return hash(self) == hash(other)
    def __ne__(self, other): return not self.__eq__(other)

    def __iter__(self): return ((key, value) for key, value in self.items())
    def __getitem__(self, key): return self.todict()[key]

    def keys(self): return tuple(self.__class__.fields)
    def values(self): return tuple(super().__iter__())
    def items(self): return tuple([(key, value) for key, value in zip(self.keys(), self.values())])
    def todict(self): return ODict([(key, value) for key, value in self.items()])

    @property
    def name(self): return self.__name


class Collection(list):
    def __init__(self, items=[]):
        super().__init__([item for item in iter(items) if item is not None])

    def append(self, values):
        assert not isinstance(values, type(self))
        if isinstance(values, list):
            super().extend(Collection(values))
        else:
            super().append(values)


class DatasetMeta(type):
    def __init__(cls, *args, reductions={}, reduction=lambda x: x, **kwargs):
        if not any([type(base) is DatasetMeta for base in cls.__bases__]):
            cls.__fields__ = {}
            cls.__reductions__ = {}
            return
        if "fields" not in kwargs.keys():
            return
        assert isinstance(kwargs["fields"], (list, dict))
        fields = kwargs["fields"] if isinstance(kwargs["fields"], dict) else {field: kwargs["type"] for field in kwargs["fields"]}
        cls.__fields__ = {key: value for key, value in cls.__fields__.items()}
        cls.__fields__.update({key: value for key, value in fields.items()})
        cls.__reductions__ = {key: value for key, value in cls.__reductions__.items()}
        cls.__reductions__.update({field: reductions.get(field, reduction) for field in fields.keys()})
        assert all([callable(reduction) for reduction in cls.__reductions__.values()])

    def __call__(cls, contents, *args, **kwargs):
        assert isinstance(contents, dict)
        collections = {field: Collection() for field in cls.__fields__.keys()}
        for field in cls.__fields__.keys():
            if field not in contents.keys():
                continue
            collections[field].append(contents[field])
        instance = super(DatasetMeta, cls).__call__(collections, *args, fields=cls.__fields__, reductions=cls.__reductions__, **kwargs)
        return instance

    def create(cls, *args, **kwargs):
        return type(cls)(cls.__name__, (cls,), {}, *args, **kwargs)


class Dataset(ODict, metaclass=DatasetMeta):
    def __init_subclass__(cls, *args, **kwargs): pass

    def __init__(self, collections, *args, name=None, reductions, fields, **kwargs):
        assert all([isinstance(collection, Collection) for collection in collections.values()])
        self.__name = name if name is not None else self.__class__.__name__
        self.__reductions = reductions
        self.__fields = fields
        super().__init__(collections)

    def __repr__(self): return "{}[{}]".format(self.name, ", ".join(["=".join([str(key), str(value.__name__)]) for key, value in self.fields()]))
    def __str__(self): return "{}: {}".format(self.name, ", ".join(["{}[{:.0f}]".format(str(key), int(value)) for key, value in zip(self.keys(), self.sizes()) if bool(value)]))
    def __bool__(self): return any([bool(collection) for collection in super().values()])

    def __iter__(self): return ((key, self.reductions[key](collection)) for key, collection in super().items() if bool(collection))
    def __contains__(self, key): return bool(super().__getitem__(key)) if key in super().keys() else False
    def __getitem__(self, key): return self.reductions[key](super().__getitem__(key)) if bool(super().__getitem__(key)) else None

    def __iadd__(self, other):
        assert isinstance(other, type(self))
        for key in self.keys():
            self[key].extend(other[key])
        return self

    def __add__(self, other):
        assert isinstance(other, type(self))
        contents = {key: self[key] + other[key] for key in self.keys()}
        return self.__class__(contents)

    @property
    def name(self): return self.__name
    @property
    def fields(self): return self.__fields
    @property
    def reductions(self): return self.__reductions

    def keys(self): return tuple(list(super().keys()))
    def indexes(self): return tuple(list(range(len(self))))
    def values(self): return tuple([reduction(values) if bool(values) else None for values, reduction in zip(super().values(), self.reductions.values())])
    def sizes(self): return tuple([len(reduction(values)) if bool(values) else 0 for values, reduction in zip(super().values(), self.reductions.values())])
    def items(self): return tuple([(key, reduction(values) if bool(values) else None) for (key, values), reduction in zip(super().items(), self.reductions.values())])
    def todict(self): return ODict([(key, value) for key, value in self.items()])
    def results(self): return Results({key: size for key, size in zip(self.keys(), self.sizes())}, name=self.name)


class Results(dict):
    def __init__(self, contents, *args, name, **kwargs):
        assert isinstance(contents, dict)
        assert all([isinstance(value, int) for value in contents.values()])
        super().__init__(contents)
        self.__name = name

    def __repr__(self): return "{}[{}]".format(self.name, ", ".join(["=".join([str(key), str(value)]) for key, value in self.items() if value > 0]))
    def __str__(self): return "{}: {}".format(self.name, ", ".join(["=".join([str(key), str(value)]) for key, value in self.items() if value > 0]))

    def __add__(self, other):
        assert isinstance(other, type(self))
        keys = list(set([*self.keys(), *other.keys()]))
        contents = {key: self.get(key, 0) + other.get(key, 0) for key in keys}
        return self.__class__(contents, name=self.name)

    @property
    def name(self): return self.__name



