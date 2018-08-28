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

'''
A Retry module that includes a retry() generator. This generator
helps encapsulate the retry/timeout logic. After each iteration,
we check if we have hit the max attempts or the timeout value.
If so, we raise the appropriate exception. The caller can specify
the amount of time to sleep between loop iterations.

Because this is a generator function, it is expected to be used in
a for loop. The caller is responsible for breaking/returning from
the for loop on success.

Examples:

>>> for attempt in dalibs.retry.retry(attemps=5, timeout=60, sleeptime=1):
>>>    do something
>>>    if some condition:
>>>        break


>>> import datetime
>>> for attempt in dalibs.retry.retry(attempts=-1, timeout=10, sleeptime=1):
...     print datetime.datetime.now()
...
2015-03-05 08:10:42.803568
2015-03-05 08:10:43.807957
2015-03-05 08:10:44.812362
2015-03-05 08:10:45.817635
2015-03-05 08:10:46.818856
2015-03-05 08:10:47.824164
2015-03-05 08:10:48.828258
2015-03-05 08:10:49.829867
2015-03-05 08:10:50.835153
2015-03-05 08:10:51.840465
2015-03-05 08:10:52.842798
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
  File "retry.py", line 72, in retry
    raise RetryTimeoutException('Timedout after %d seconds!' % timeout)
retry.RetryTimeoutException: Timedout after 10 seconds!

'''

import time


class RetryException(Exception):
    pass


class RetryTimeoutException(RetryException):
    pass


class RetryAttemptsException(RetryException):
    pass


def retry(attempts=-1, timeout=-1, sleeptime=1, raises=True, message=None):
    _attempts = 0
    endtime = None
    if timeout > 0:
        endtime = time.time() + timeout
    while True:
        if endtime and time.time() > endtime:
            if message is None:
                message = 'Timedout after %d seconds!' % timeout
            if raises:
                raise RetryTimeoutException(message)
            raise StopIteration(message)
        _attempts += 1
        yield _attempts
        if _attempts == attempts:
            if message is None:
                message = 'Retried unsuccessfully %d times!' % attempts
            if raises:
                raise RetryAttemptsException(message)
            raise StopIteration(message)
        if endtime and time.time() > endtime:
            if message is None:
                message = 'Timedout after %d seconds!' % timeout
            if raises:
                raise RetryTimeoutException(message)
            raise StopIteration(message)
        if endtime:
            time_remaining = endtime - time.time()
            min_time = min(time_remaining, sleeptime)
            if min_time > 0:
                time.sleep(min_time)
        else:
            time.sleep(sleeptime)
