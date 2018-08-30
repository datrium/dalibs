#!/usr/bin/env python
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

from setuptools import setup

setup(
    name="dalibs",
    version="1.0",
    description='Datrium Python Common Libraries',
    author='Kyle Harris <kyle@datrium.com>, Anupam Garg <angarg@gmail.com>',
    packages=['dalibs',],
    install_requires=[
        'ecdsa>=0.13',
        'paramiko>=1.15.2',
        'scp>=0.8.0',
        'subprocess32>=3.2.6',
    ],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Topic :: Software Development :: Libraries',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ],
    keywords='lib decorators cached retry',
    project_urls={
        'Bug Reports': 'https://github.com/datrium/dalibs/issues',
        'Source': 'https://github.com/datrium/dalibs',
    },
)
