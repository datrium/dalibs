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
Module that extends Python Threads with the following:
1. a global exception list for all threads so that we can later
   determine thread exception ordering
2. thread exception logging using python logging infrastructure instead
   of just printing to sys.stderr
3. ability to abort threads (with caveats)
'''

import collections
import ctypes
import datetime
import logging
import os
import threading
import traceback
import retry
import signal
import sys


class ThreadAbortedException(threading.ThreadError):
    pass


ThreadExceptionEntry = collections.namedtuple('ThreadExceptionEntry', ['tid', 'timestamp', 'exc_info'])
exceptions_list = []


def get_first_exception_entry():
    if exceptions_list:
        exceptions_list.sort(key=lambda x: x.timestamp)
        return exceptions_list[0]
    return None


def reset_exceptions():
    global exceptions_list
    exceptions_list = []


# Install signal handler for SIGUSR1. Invoke that signal when any helper thread hits an exception.
# Python handles all signals in the main thread and in the handler we will check for pending exceptions,
# log it, and die. If SIGUSR1 turns out to be used for something else, we can probably manage with TERM.
def sig_handler(signum, frame):
    exc_entry = get_first_exception_entry()
    if exc_entry:
        exc_info = exc_entry.exc_info
        logging.error('Caught signal from a helper thread due to exception on it')
        tb = ''.join(traceback.format_tb(exc_info[2]))
        msg = tb.splitlines()
        msg += [str(exc_info[1])]
        logging.error('Helper thread error: %s' % msg)
        raise exc_info[0], exc_info[1], exc_info[2]


try:
    old = signal.signal(signal.SIGUSR1, sig_handler)
    if old:
        logging.debug('Overrode existing signal handler for SIGUSR1: %s' % str(old))
except Exception as e:
    logging.exception('Unable to override signal handler for SIGUSR1')


class ThreadException(threading.Thread):
    ''' Thread subclass what will catch and log exceptions '''
    def __init__(self, *args, **kwargs):
        self.do_signal = kwargs.pop('do_signal', False)
        self.do_logging = kwargs.pop('do_logging', bool(int(os.environ.get('DA_PYTHON_STACKTRACE_TO_STDERR', '1'))))
        # report=False results in the thread registering the exception, but not reporting it.
        # The main thread will still raise the exception, however, when joining if not caught.
        # This is useful for not printing the backtrace to the console and excluding the exception
        # from the exceptionList.
        self.report = kwargs.pop('report', True)
        threading.Thread.__init__(self, *args, **kwargs)
        self._parent = threading.current_thread()
        self._aborted_by = None
        self.exc_info = None
        self.daemon = True

    @property
    def tid(self):
        '''
        :return: The thread_id of this thread. Needed by the abort path.
        '''
        if not self.is_alive():
            return None
        if hasattr(self, '_thread_id'):
            return self._thread_id
        for tid, tobj in threading._active.items():
            if tobj is self:
                self._thread_id = tid
                return tid
        return None

    def run(self):
        try:
            super(ThreadException, self).run()
        except Exception as e:
            self.exc_info = sys.exc_info()
            if self._aborted_by is not None:
                return
            if self.report:
                entry = ThreadExceptionEntry(tid=self.tid, timestamp=datetime.datetime.now(), exc_info=self.exc_info)
                exceptions_list.append(entry)
                if self.do_signal:
                    os.kill(os.getpid(), signal.SIGUSR1)
                raise  # probably not necessary but doesnt hurt either.

    def join(self, *args):
        super(ThreadException, self).join(*args)
        if self.exc_info and not self.aborted:
            exc_info = self.exc_info
            raise exc_info[0], exc_info[1], exc_info[2]

    @property
    def aborted(self):
        '''
        :return: True if the thread was aborted, and False otherwise
        '''
        return not self.is_alive() and self._aborted_by is not None

    def abort(self, timeout=None):
        '''
        This is not really supported. Perform a google search for "abortable Python threads"
        for a complete history with many various opinions. We are introducing the functionality.
        Use it sparingly.
        '''
        for _ in retry.retry(timeout=timeout, sleeptime=0.1, raises=False):
            tid = self.tid
            if tid is None:
                return
            self._aborted_by = threading.current_thread()
            ret = ctypes.pythonapi.PyThreadState_SetAsyncExc(ctypes.c_long(tid),
                                                             ctypes.py_object(ThreadAbortedException))
            if ret == 0:
                raise RuntimeError('Invalid thread: %s' % tid)
            if ret > 1:
                ctypes.pythonapi.PyThreadState_SetAsyncExc(ctypes.c_long(tid), None)
                raise SystemError('PyThreadState_SetAsyncExc failed')
            self.join(0.1)
            if self.aborted:
                logging.debug('Thread %s aborted by %s' % (self.name, self._aborted_by.name))
                return

    def _Thread__bootstrap_inner(self):
        '''
        Taken from python2.7 threading.py _with modifications_.

        The reason to take this from the stdlib is so that we can override the exception printing.
        We'd like to use our own logger to do this instead of printing to stderr.

        The modifications were to add a toggle to either log.debug the exception, or to ignore
        it completely. Use DA_PYTHON_STACKTRACE_TO_STDERR to toggle the behavior. By default, the
        behavior is 1/on, meaning exceptions will get logged with DEBUG level from here. If you
        toggle with 0/off, then the exception won't get logged here. Eventually we should completely
        supress the exception from here, because it often makes debugging harder. We usually catch
        the exception, and reraise or print a compact message. Eliminating this stderr output likely
        reduces unnecessary noise.

        Additional modifications were to properly reference all __private vars and methods, and
        to reference threading global variables.
        '''
        try:
            self._set_ident()
            self._Thread__started.set()
            with threading._active_limbo_lock:
                threading._active[self._Thread__ident] = self
                del threading._limbo[self]
            if __debug__:
                self._note('%s.__bootstrap(): thread started', self)

            if threading._trace_hook:
                self._note('%s.__bootstrap(): registering trace hook', self)
                threading._sys.settrace(threading._trace_hook)
            if threading._profile_hook:
                self._note('%s.__bootstrap(): registering profile hook', self)
                threading._sys.setprofile(threading._profile_hook)

            try:
                self.run()
            except SystemExit:
                if __debug__:
                    self._note('%s.__bootstrap(): raised SystemExit', self)
            except:
                if __debug__:
                    self._note('%s.__bootstrap(): unhandled exception', self)
                # If sys.stderr is no more (most likely from interpreter
                # shutdown) use self.__stderr.  Otherwise still use sys (as in
                # _sys) in case sys.stderr was redefined since the creation of
                # self.
                if threading._sys:
                    # MODIFIED by Datrium
                    if self.do_logging:
                        logging.debug('Exception in thread %s:\n%s\n' % (self.name, threading._format_exc()))
                else:
                    # Do the best job possible w/o a huge amt. of code to
                    # approximate a traceback (code ideas from
                    # Lib/traceback.py)
                    exc_type, exc_value, exc_tb = self._Thread__exc_info()
                    try:
                        print>>self._Thread__stderr, (
                            'Exception in thread ' + self.name +
                            ' (most likely raised during interpreter shutdown):')
                        print>>self._Thread__stderr, (
                            'Traceback (most recent call last):')
                        while exc_tb:
                            print>>self._Thread__stderr, (
                                '  File '%s', line %s, in %s' %
                                (exc_tb.tb_frame.f_code.co_filename,
                                    exc_tb.tb_lineno,
                                    exc_tb.tb_frame.f_code.co_name))
                            exc_tb = exc_tb.tb_next
                        print>>self._Thread__stderr, ('%s: %s' % (exc_type, exc_value))
                    # Make sure that exc_tb gets deleted since it is a memory
                    # hog; deleting everything else is just for thoroughness
                    finally:
                        del exc_type, exc_value, exc_tb
            else:
                if __debug__:
                    self._note('%s.__bootstrap(): normal return', self)
            finally:
                # Prevent a race in
                # test_threading.test_no_refcycle_through_target when
                # the exception keeps the target alive past when we
                # assert that it's dead.
                self._Thread__exc_clear()
        finally:
            with threading._active_limbo_lock:
                self._Thread__stop()
                try:
                    # We don't call self.__delete() because it also
                    # grabs _active_limbo_lock.
                    del threading._active[threading._get_ident()]
                except:
                    pass
