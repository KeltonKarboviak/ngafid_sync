# -*- coding: utf-8 -*-

from collections import namedtuple


class Organization(namedtuple('Organization', ['name', 'token'])):
    __slots__ = ()

    @property
    def is_und_org(self) -> bool:
        return self.name.startswith('UND')
