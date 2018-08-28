#
#
# Copyright (c) 2013-2018 Datrium Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

class cached(object):
    '''
    Decorator to cache expensive class properties

    Acts like an @property annotation.
    Example Usage:
        In [1]: class CachedContainer(object):
        ...:     @dalibs.decorators.cached
        ...:     def foo(self):
        ...:         return 'a'
        ...:

        In [2]: c = CachedContainer()

        In [3]: c.foo
        Out[3]: 'a'

        In [4]: c.foo
        Out[4]: 'a'

        In [5]: c.__dict__
        Out[5]: {'foo': 'a'}
    '''
    def __init__(self, obj, attr=None):
        self._attr = attr or obj.__name__
        self._obj = obj

    def __get__(self, instance, owner):
        attr = self._obj(instance)
        instance.__dict__[self._attr] = attr
        return attr
