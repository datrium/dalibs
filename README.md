# DaLibs

A set of useful, general-purpose libraries we use at [Datrium](http://www.datrium.com). Some of the
most commonly used libs are the (1) retry module, (2) the cached decorator, and (3) the ssh module exposing a [subprocess](https://docs.python.org/2/library/subprocess.html)-style interface
over ssh.

## Install
```
pip install --process-dependency-links https://github.com/datrium/dalibs/archive/master.zip#egg=dalibs
```

---
## retry
A generator that encapsulates retry and timeout logic. Examples:

```
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
```

---
## ssh
An ssh module that implements enough of a [subprocess](https://docs.python.org/2/library/subprocess.html) style interface to allow us to write the same code for different backends.
Examples:

```
import subprocess
import ssh

def backend(self):
    if self._is_remote:
        return ssh
    return subprocess

p = someobj.backend.Popen(...)
p.communicate()
```

# Contributors
The original source for this repository is a private git repository
at Datrium Inc. Because of that, GitHub commit history does not show
contributions made by Datrium employees over time. This section is used
to give credit to those that have contributed to this project internally
at Datrium Inc.

* Anupam Garg, [@angarg](https://github.com/angarg)
* E Lewis, [@eclewis](https://github.com/eclewis)
* Kyle Harris, [@khhjr](https://github.com/khhjr)
* Mark Huang, [@vermaport](https://github.com/vermaport)
* Nitin Garg, [@ngrg](https://github.com/ngrg)
