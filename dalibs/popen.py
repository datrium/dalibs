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
This module provides a Popen class and friends that can be customized.
Currently supports timeout.
'''

import errno
import os
import signal
import subprocess as subprocess27
import subprocess32
import thread
import threading
import time

# we want to expose everything subprocess32 has
# because most consumers will use this like so:
# import popen as subprocess
from subprocess32 import *


class TimeoutExpired(subprocess32.TimeoutExpired):
    def __init__(self, cmd, timeout, pid=None, output=None):
        super(TimeoutExpired, self).__init__(cmd, timeout, output)
        self.pid = pid


class CalledProcessError(subprocess27.CalledProcessError, subprocess32.CalledProcessError):
    def __init__(self, returncode, cmd, pid=None, output=None):
        super(CalledProcessError, self).__init__(returncode, cmd, output)
        self.pid = pid

    def __str__(self):
        msg = "Command '%s' returned non-zero exit status %d" % (self.cmd, self.returncode)
        if self.output is not None:
            msg += ': %s' % self.output
        return msg


def call(*popenargs, **kwargs):
    timeout = kwargs.pop('timeout', None)
    p = Popen(*popenargs, **kwargs)
    return p.wait(timeout=timeout)


def check_call(*popenargs, **kwargs):
    timeout = kwargs.pop('timeout', None)
    p = Popen(*popenargs, **kwargs)
    p.check_wait(timeout=timeout)
    return 0


def check_output(*popenargs, **kwargs):
    if 'stdout' in kwargs:
        raise ValueError('stdout argument not allowed, it will be overridden.')
    timeout = kwargs.pop('timeout', None)
    p = Popen(*popenargs, stdout=subprocess32.PIPE, **kwargs)
    output, unused_err = p.communicate(timeout=timeout)
    retcode = p.poll()
    if retcode:
        raise CalledProcessError(retcode, p.args, output=output, pid=p.pid)
    return output


class LockPlaceHolder(object):
    # A placeholder for a pickled lock.
    pass


class Popen(subprocess32.Popen):
    def __init__(self, *pargs, **kwargs):
        # The default timeout for any Da process is 1 hr. Caller must override this
        # if a process is expected to take longer than that.
        self.signal = kwargs.pop('signal', signal.SIGABRT)
        self.starttime = time.time()
        # In the event wait() is later called w/o timeout, the duration is the active timeout.
        self.timeout = kwargs.pop('timeout', 1*60*60)
        self.endtime = None
        if self.timeout is not None:
            self.endtime = self.starttime + self.timeout
        super(Popen, self).__init__(*pargs, **kwargs)

    def __getstate__(self):
        # Replace locks with placeholders.
        return dict([(k, v if not isinstance(v, thread.LockType) else LockPlaceHolder())
                     for k, v in self.__dict__.iteritems()])

    def __setstate__(self, d):
        # Replace placeholder locks with true lock.
        self.__dict__ = dict([(k, v if not isinstance(v, LockPlaceHolder) else threading.Lock())
                              for k, v in d.iteritems()])

    def die(self):
        self.endtime = None  # reset endtime so that we don't raise TimeoutExpired yet
        killed = False
        end_at = time.time() + 10*60  # force kill after 10 minutes
        # Try and get the core from the child too.
        try:
            child_pid = int(subprocess32.check_output(['pgrep', '-P', str(self.pid)]))
        except subprocess32.CalledProcessError:
            child_pid = 0
        try:
            if child_pid != 0:
                os.kill(child_pid, self.signal)
            self.send_signal(self.signal)
        except OSError as e:
            if e.errno == errno.ESRCH:  # No such process; already dead
                return
            raise
        while not killed and time.time() < end_at:
            killed = super(Popen, self).poll() is not None
            time.sleep(1)
        if super(Popen, self).poll() is None:
            self.kill()

    def wait(self, signal=None, **kwargs):
        # signal param is for this call only
        # store the original signal, and if this signal is not tripped, restore the original later
        _orig_signal = self.signal
        self.signal = signal or self.signal
        # subprocess32.Popen.wait wants to use endtime; timeout is only passed in for printing.
        # There are 2 cases to handle:
        # 1. Popen got the original timeout. In that case, the endtime is still self.endtime, and
        #    timeout is likely self.timeout. If/when timeout occurs, the exception states that we
        #    timedout in self.timeout seconds (which is self.endtime - self.starttime)
        # 2. this wait() got timeout. In this case, we need to adjust self.endtime.
        #    Instead of adjusting self.timeout, we just pass the new timeout to the super wait()
        #    in order to get the correct printing.
        timeout = kwargs.pop('timeout', None)
        if timeout is not None:
            self.endtime = time.time() + timeout
        if self.endtime is not None:
            timeout = timeout or self.timeout
        try:
            r = super(Popen, self).wait(timeout=timeout, endtime=self.endtime, **kwargs)
            self.signal = _orig_signal
            return r
        except subprocess32.TimeoutExpired as e:
            self.die()
            raise TimeoutExpired(e.cmd, e.timeout, output=e.output, pid=self.pid)

    def check_wait(self, timeout=None):
        retcode = self.wait(timeout=timeout)
        if retcode:
            raise CalledProcessError(retcode, self.args, pid=self.pid)

    def poll(self):
        if self.endtime is not None:
            if time.time() > self.endtime:
                self.die()
                raise TimeoutExpired(self.args, self.timeout, pid=self.pid)
        return super(Popen, self).poll()

    def communicate(self, *args, **kwargs):
        try:
            return super(Popen, self).communicate(*args, **kwargs)
        except subprocess32.TimeoutExpired as e:
            self.die()
            raise TimeoutExpired(e.cmd, e.timeout, output=e.output, pid=self.pid)
