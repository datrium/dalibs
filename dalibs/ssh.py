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

'''
An ssh module that implements enough of a Popen and family
style interface over ssh. It allows us to write code a little
generically, for different contexts, like this:

import subprocess
import ssh

def backend(self):
    if self._is_remote:
        return ssh
    return subprocess

p = someobj.backend.Popen(...)
p.communicate()

'''

import cStringIO
import inspect
import logging
import os
import paramiko
import popen as subprocess
import Queue
import random
import retry
import scp
import socket
import sys
import threading
import threads
import time


logging.getLogger('paramiko.transport').setLevel(logging.WARN)


GLOBAL_CONNECTION_TAG = None
def set_global_connection_tag(tag):
    '''
    Some callers may desire multiplexed sessions. Setting the global tag
    achieves this w/o passing a tag with every ssh call.
    '''
    global GLOBAL_CONNECTION_TAG
    GLOBAL_CONNECTION_TAG = tag


def ignore_during_ssh_stack_logging(f):
    '''
    This ssh module implements a custom logger that inspects the stack to log
    the most appropriate filename, line number, and function name. See __makeRecord
    for more information.

    The user can also use this decorator to tell __makeRecord to skip the intended method.
    For example, we could have code that looks like this:
            def version(self):
                try:
                    cmd = 'echo 1.1.1'
                    return self.execute(cmd).strip()
                except subprocess.CalledProcessError:
                    return None
    and, a execute method that looks like this:
            def execute(self, *args, **kwargs):
                return ssh.check_output(self.ipaddr, cmd, name=self.rolename, **kwargs)

    If we call obj.version, the log line would look like this:
    2017-01-05 14:27:45,817 file.py:2286  DEBUG  execute USER@HOST: executing "echo 1.1.1"

    The log line does not tell us who called execute. In most cases, when looking through logs,
    it is more helpful to know that we were in the version() method. The writer could use this
    decorator to instruct the custom ssh logger to skip a method, and instead find a function
    earlier on the call stack.

    For example, if we had this:
            @ssh.ignore_during_ssh_stack_logging
            def execute(self, *args, **kwargs):
                return ssh.check_output(self.ipaddr, cmd, name=self.rolename, **kwargs)

    The corresponding log line that would get printed would be:
    2017-01-05 16:14:48,251 file.py:1497  DEBUG  version USER@HOST: executing "echo 1.1.1"
    2017-01-05 16:14:49,689 file.py:1497  DEBUG  version USER@HOST: "echo 1.1.1" returned 0
    2017-01-05 16:14:49,841 file.py:1497  DEBUG  version USER@HOST: stdout: 1.1.1
    2017-01-05 16:14:49,891 file.py:1497  DEBUG  version USER@HOST: stderr: 1.1.1
    '''
    def _ignore_during_ssh_stack_logging(*args, **kwargs):
        return f(*args, **kwargs)
    return _ignore_during_ssh_stack_logging


def __makeRecord(name, level, fn, lno, msg, args, exc_info, func=None, extra=None):
    '''
    Custom makeRecord makes custom LogRecords. We modify the original info to put
    the caller's fn and lno into the LogRecord, so that formatting can display the
    caller instead of this module. This is helpful since we often want to know who,
    or is what context, something is failing.

    For example, without this change, we get loglines like this:
    2016-03-15T23:23:57.548423+0000     DEBUG   ssh.py:416  --  admin@None: connecting
    2016-03-15T23:23:57.548742+0000   WARNING   ssh.py:403  --  Failed to open channel to admin@None: Could not connect to admin@None

    It is better to know where the call is coming from in order to determine why the
    ipaddr is None, and what it is trying to do.
    '''
    def wanted(frame):
        # ignore stdlib paths
        if os.path.dirname(frame[1]).startswith(os.path.dirname(logging.__path__[0])):
            return False
        if os.path.basename(frame[1]) in ['ssh.py', 'scp.py', 'threads.py']:
            return False
        # skip module name
        if frame[1].startswith('<'):
            return False
        # skip the _ignore_during_ssh_stack_logging decorator
        if frame[3] == '_ignore_during_ssh_stack_logging':
            return False
        # skip the _ignore_during_ssh_stack_logging decorator wrapped function
        if frame[0].f_back and frame[0].f_back.f_code.co_name == '_ignore_during_ssh_stack_logging':
            return False
        return True

    try:
        t = threading.current_thread()
        while t is not None:
            stack = sys._current_frames()[t.ident]
            for frame in inspect.getouterframes(stack):
                try:
                    if wanted(frame):
                        fn = os.path.basename(frame[1])
                        lno = frame[2]
                        func = frame[3]
                        t = None  # reset so we don't continue
                        break
                finally:
                    del frame
            # traverse to parent threads, if possible
            if t is not None and hasattr(t, '_parent') and t._parent is not None:
                t = t._parent
            else:
                t = None
    except:
        # we don't want exceptions to stop logging, so just pass through
        pass

    rv = logging.LogRecord(name, level, fn, lno, msg, args, exc_info, func)
    if extra is not None:
        for key in extra:
            if key in ['message', 'asctime'] or key in rv.__dict__:
                raise KeyError('Attempt to overwrite %r in LogRecord' % key)
            rv.__dict__[key] = extra[key]
    return rv


logger = logging.getLogger('ssh')
logger.makeRecord = __makeRecord
logger.addHandler(logging.NullHandler())


class DummyLogger(object):
    '''
    Simple dummy logger interface used when clients pass in do_logging=False.
    '''
    def debug(self, *args, **kwargs):
        pass

    def info(self, *args, **kwargs):
        pass

    def warn(self, *args, **kwargs):
        pass

    def warning(self, *args, **kwargs):
        pass

    def error(self, *args, **kwargs):
        pass

    def exception(self, *args, **kwargs):
        pass


class CalledProcessAborted(subprocess.CalledProcessError):
    def __init__(self, returncode, cmd, pid=None, output=''):
        super(CalledProcessAborted, self).__init__(returncode, cmd, output)
        self.pid = pid

    def __str__(self):
        return 'Command \'%s\' was aborted' % self.cmd


class HostConnectError(subprocess.CalledProcessError):
    def __init__(self, username, hostname, returncode, cmd, output='', reason=None):
        super(HostConnectError, self).__init__(returncode, cmd, output)
        self.username = username
        self.hostname = hostname
        self.reason = reason
        # SSHException exposes message, define that also for consistency.
        self.message = reason

    def __str__(self):
        s = 'Could not connect to %s@%s' % (self.username, self.hostname)
        if self.reason is not None:
            s += ' because %s' % str(self.reason)
        return s


class InvalidHostnameError(HostConnectError):
    pass


# Expose paramiko's SSHException through this module. Most consumers don't know we wrap
# around paramiko. It also lets us swap the backend in the future if needed.
# NOTE: callers should catch ssh.SSHException instead of paramiko.ssh_exception.SSHException
def reason(self):
    return self.message
SSHException = paramiko.ssh_exception.SSHException
paramiko.ssh_exception.SSHException.reason = property(reason)


def shell_cmd(hostname, *args, **kwargs):
    ''' Convenience function for running a synchronous SSH command. '''
    ssh = SSH(hostname, *args, **kwargs)
    ssh.start()
    while ssh.is_alive():
        try:
            time.sleep(0.1)
        except KeyboardInterrupt:
            ssh.abort = True
    ssh.join()
    return ssh


def get(hostname, src, dst, **kwargs):
    '''
    Convenience function for running a synchronous get command.
    Operation is recursive for directories.
    '''
    return _scp(hostname, src, dst, mode='get', **kwargs)


def put(hostname, src, dst, **kwargs):
    '''
    Convenience function for running a synchronous put command.
    Operation is recursive for directories.
    '''
    return _scp(hostname, src, dst, mode='put', **kwargs)


def _scp(hostname, src, dst, mode, **kwargs):
    scp = SCP(hostname, src, dst, mode, **kwargs)
    scp.start()
    while scp.is_alive():
        try:
            time.sleep(0.1)
        except KeyboardInterrupt:
            scp.abort = True
    scp.join()
    return scp


# Popen type functions.
def call(hostname, *args, **kwargs):
    s = shell_cmd(hostname, *args, **kwargs)
    return s.exit_code


def check_call(hostname, *args, **kwargs):
    s = shell_cmd(hostname, *args, **kwargs)
    if s.exit_code != 0:
        raise subprocess.CalledProcessError(s.exit_code, s.cmd)
    return s.exit_code


def check_output(hostname, *args, **kwargs):
    if 'stdout' in kwargs:
        raise ValueError('stdout argument not allowed, it will be overridden.')
    kwargs['stdout'] = subprocess.PIPE
    if 'stderr' not in kwargs:
        kwargs['stderr'] = subprocess.PIPE
    s = shell_cmd(hostname, *args, **kwargs)
    stdout = s.stdout.read()
    if s.exit_code != 0:
        raise subprocess.CalledProcessError(s.exit_code, s.cmd, output=stdout)
    return stdout


class Popen(object):
    ''' Implements (limited) Popen to a host '''
    def __init__(self, hostname, *args, **kwargs):
        kwargs.update({'pty': False})
        self.ssh = SSH(hostname, *args, **kwargs)
        self.ssh.start()
        self.stdin = DummyStdin(self.ssh)
        # TODO Support pid.
        self.pid = None

    def __getattr__(self, name):
        return getattr(self.ssh, name)

    def communicate(self, stdin=None, timeout=None):
        self.ssh.update_timeout(timeout)
        if stdin:
            self.stdin.write(stdin)
        self.wait()
        stdout = None
        stderr = None
        if isinstance(self.stdout, Pipe):
            stdout = self.ssh.stdout.read()
        if isinstance(self.stderr, Pipe):
            stderr = self.ssh.stderr.read()
        return stdout, stderr

    def terminate(self, *args, **kwargs):
        self.ssh.abort = True

    def send_signal(self, *args, **kwargs):
        pass

    def kill(self):
        pass

    def poll(self):
        if self.ssh.is_alive():
            return None
        self.ssh.join()
        return self.ssh.exit_code

    def wait(self, timeout=None, signal=None):
        self.ssh.update_timeout(timeout)
        status = self.poll()
        while status is None:
            try:
                time.sleep(0.1)
                status = self.poll()
            except KeyboardInterrupt:
                self.ssh.abort = True
        return status

    @property
    def returncode(self):
        return self.wait()


class Connection(object):
    ''' Connection container '''
    def __init__(self, connection, key):
        self.connection = connection
        self.key = key
        self.count = 0

    # Counting relies on lock from caller.
    def dec(self):
        if self.count > 0:
            self.count -= 1

    def inc(self):
        self.count += 1

    def close(self):
        try:
            self.connection.close()
        except:
            pass


class Connections(object):
    ''' Mapping for caching SSH connections. '''
    def __init__(self, *args, **kwargs):
        self.lock = threading.Lock()
        self.connections = {}

    def _key(self, username, hostname, tag):
        return '%s@%s%s' % (username, hostname, tag)

    def insert(self, username, hostname, client, tag=None):
        ''' Insert a connection into the cache and return one '''
        key = self._key(username, hostname, tag)
        with self.lock:
            if key in self.connections:
                connection = self.connections[key]
                if connection.count == 0:
                    connection.close()
                    self.connections[key] = Connection(client, key)
                else:
                    # In the unlikely event there were 2 connections made at roughly the same
                    # time, prefer the newer and attempt to close the older.
                    try:
                        client.close()
                    except:
                        pass
            else:
                self.connections[key] = Connection(client, key)
            self.connections[key].inc()
            return self.connections[key]

    def get(self, username, hostname, tag=None):
        ''' Acquire a connection '''
        key = self._key(username, hostname, tag)
        connection = None
        with self.lock:
            if key in self.connections:
                connection = self.connections[key]
                assert connection.key == key
                connection.inc()
            return connection

    def put(self, username, hostname, tag=None):
        ''' Release a connection '''
        key = self._key(username, hostname, tag)
        with self.lock:
            if key in self.connections:
                connection = self.connections[key]
                assert connection.key == key
                connection.dec()

    def remove(self, username, hostname, tag=None):
        ''' Remove a connection '''
        with self.lock:
            key = self._key(username, hostname, tag)
            connection = self.connections.get(key, None)
            if connection is not None:
                assert connection.key == key
                if connection.count == 0:
                    logger.debug('%s: Closing connection.' % key)
                    connection.close()
                    del self.connections[key]

    def clear(self):
        ''' Clear the entire cache '''
        # N.B. This doesn't check for active connections.
        with self.lock:
            for connection in self.connections.values():
                connection.close()
            self.connections.clear()


cache = Connections()


import atexit
atexit.register(cache.clear)


class Pipe(object):
    ''' A Pipe implementation using the StingIO() function '''
    def __init__(self):
        self._buf = cStringIO.StringIO()
        self._lock = threading.Lock()
        self._rpos = 0
        self._wpos = 0

    def __getattr__(self, name):
        if hasattr(self._buf, name):
            return getattr(self._buf, name)
        raise AttributeError('Unknown attribute: %s' % name)

    def __str__(self):
        with self._lock:
            # Return the entire buffer with str, mainly for debugging and exception handling.
            self._buf.seek(0)
            return self._buf.read()

    def readline(self):
        with self._lock:
            self._buf.seek(self._rpos)
            buf = self._buf.readline()
            self._rpos = self._buf.tell()
            return buf

    def read(self):
        with self._lock:
            self._buf.seek(self._rpos)
            buf = self._buf.read()
            self._rpos = self._buf.tell()
            return buf

    def readlines(self):
        with self._lock:
            self._buf.seek(self._rpos)
            buf = self._buf.readlines()
            self._rpos = self._buf.tell()
            return buf

    def write(self, buf):
        with self._lock:
            # writes always get appended.
            self._buf.seek(self._wpos)
            self._buf.write(buf)
            self._buf.flush()
            self._wpos = self._buf.tell()


class SSHClient(threads.ThreadException):
    def __init__(self, hostname, *args, **kwargs):
        self.do_logging = kwargs.pop('do_logging', True)
        self.logger = logger
        if not self.do_logging:
            self.logger = DummyLogger()
        self.do_report = kwargs.get('report', self.do_logging)
        super(SSHClient, self).__init__(target=self._execute, do_logging=self.do_logging, report=self.do_report)

        # convert threadname from Thread-13 to 13-Controller0
        name = kwargs.pop('name', None)
        if name is not None:
            self.name = self.name.split('-')[1] + '-' + name
        self.daemon = True
        self.args = args
        self.kwargs = kwargs
        self.hostname = hostname
        self.username = kwargs.get('username')
        self.password = kwargs.get('password')
        self.port = kwargs.get('port', 22)
        self.options = kwargs.pop('options', {})
        self.connection_tag = kwargs.pop('connection_tag', threading.current_thread().name)
        if GLOBAL_CONNECTION_TAG is not None:
            self.connection_tag = GLOBAL_CONNECTION_TAG
        self.connection_name = '%s@%s' % (self.username, self.hostname)
        if self.connection_tag is not None:
            self.connection_name = '%s@%s(%s)' % (self.username, self.hostname, self.connection_tag)
        self.cmd = None
        self._abort = False
        self._stdout = kwargs.pop('stdout', sys.stdout)
        if self._stdout == subprocess.PIPE:
            self._stdout = Pipe()
        self._stderr = kwargs.pop('stderr', sys.stderr)
        if self._stderr == subprocess.PIPE:
            self._stderr = Pipe()
        if self._stderr == subprocess.STDOUT:
            self._stderr = self._stdout
        # python threading.Thread has exception handlers that do something like this:
        # print >>self.__stderr
        # we want those to go to our stderr, not sys.stderr
        self._Thread__stderr = self._stderr
        self.exit_code = -1
        self.starttime = time.time()
        self.timeout = self.kwargs.pop('timeout', 1*60*60)
        self.connect_timeout = self.kwargs.pop('connect_timeout', 180)
        self.max_connect_attempts = self.kwargs.get('max_connect_attempts')
        if self.timeout is not None:
            # finite duration
            self.endtime = self.starttime + self.timeout
        else:
            # infinite duration
            self.endtime = None
        if self.hostname is None:
            raise InvalidHostnameError(self.username, self.hostname, self.exit_code, self.cmd, reason='Invalid hostname.')

    @property
    def stdout(self):
        return self._stdout

    @property
    def stderr(self):
        return self._stderr

    def update_timeout(self, timeout):
        if timeout is not None:
            self.timeout = timeout
            self.endtime = time.time() + timeout
            self.logger.debug('Updated timeout: %d endtime: %d' % (self.timeout, int(self.endtime)))

    def run(self):
        try:
            super(SSHClient, self).run()
        finally:
            cache.put(self.username, self.hostname, self.connection_tag)

    @property
    def client(self):
        _client = cache.get(self.username, self.hostname, self.connection_tag)
        if _client is None:
            _client = cache.insert(self.username, self.hostname, self._connect(), self.connection_tag)
        return _client

    @property
    def channel(self):
        endtime = time.time() + self.connect_timeout
        attempts = self.max_connect_attempts
        reason = None
        for _ in retry.retry(attempts=attempts, timeout=self.connect_timeout, raises=False):
            try:
                self.transport = self.client.connection.get_transport()
                self.transport.set_keepalive(10)
                return self.transport.open_session()
            except Exception as e:
                self.logger.warn('Failed to open channel to %s@%s: %s' % (self.username, self.hostname, e))
                reason = str(e)
                cache.put(self.username, self.hostname, self.connection_tag)
                cache.remove(self.username, self.hostname, self.connection_tag)
                if endtime > time.time():
                    # XXX(kyle) This should be rare. But there is a potential issue here if
                    # 2 or more threads are attempting to establish the same connection and
                    # failing. The reference count may not go to 0. Random sleep may help.
                    time.sleep(random.uniform(0, 2))
            if self._abort:
                raise CalledProcessAborted(self.exit_code, self.cmd)
        raise HostConnectError(self.username, self.hostname, self.exit_code, self.cmd, reason=reason)

    def _connect(self):
        self.logger.debug('%s: connecting' % self.connection_name)
        if self.hostname is None:
            raise HostConnectError(self.username, self.hostname, self.exit_code, self.cmd, reason='Invalid hostname.')
        _client = paramiko.SSHClient()
        _client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        _client.connect(self.hostname, self.port, self.username, self.password, timeout=self.connect_timeout,
                        **self.options)
        self.logger.debug('%s: connected' % self.connection_name)
        return _client

    def _execute(self):
        raise NotImplementedError()


class SSH(SSHClient):
    def __init__(self, hostname, *args, **kwargs):
        super(SSH, self).__init__(hostname, *args, **kwargs)
        # self.do_logging and self.logger already set in super()__init__()
        self.stdin = Queue.Queue()
        self.send_eof = True
        self.source_profile = self.username == 'root' and kwargs.get('source_profile', True)
        self.cmd = self._args_to_shell(*self.args, **self.kwargs)

    @property
    def abort(self):
        return self._abort

    @abort.setter
    def abort(self, value):
        self._abort = bool(value)

    def _execute(self):
        chan = self.channel
        try:
            if self.kwargs.get('pty', True):
                # running with psuedo-tty should terminate process
                # when channel closes (e.g., abort). If a process is
                # is pushed to background, pty is probably not desired.
                # NOTE Should pty be automatically set depending on this?
                if self.cmd.rstrip()[-1] == '&':
                    self.logger.debug('command %s is being ran with a pty' % self.cmd)
                chan.get_pty()
            self.logger.debug('%s: executing "%s"' % (self.connection_name, self.cmd))

            # NOTE: Non-interactive SSH sessions do not
            # source /etc/profile. One workaround is to maintain a parallel set
            # of environment variables in ~/.ssh/environment. This would require
            # setting "PermitUserEnvironment yes" in sshd_config, andmaintaining
            # a properly sourced env. Both of these would require some amount of
            # work to implement, maybe later.
            if self.source_profile is True:
                chan.exec_command('source /etc/profile;%s' % self.cmd)
            else:
                chan.exec_command(self.cmd)
            while chan and not chan.exit_status_ready():
                if chan.recv_ready():
                    self._stdout.write(chan.recv(10240))
                    self._stdout.flush()
                if chan.recv_stderr_ready():
                    self._stderr.write(chan.recv_stderr(10240))
                    self._stderr.flush()
                if not self.stdin.empty():
                    stdin = self.stdin.get()
                    self.logger.debug('stdin: %s' % stdin)
                    chan.sendall(stdin)
                    # The send_eof attribute indicates whether EOF should be
                    # sent following the write. This supports Popen.communicate() semantics
                    # where data is sent followed by EOF. If send_eof is False, the caller
                    # will need to call shutdown_write() if/when desired.
                    if self.send_eof:
                        chan.shutdown_write()
                if self.endtime and time.time() > self.endtime:
                    raise subprocess.TimeoutExpired(self.cmd, self.timeout)
                elif self._abort:
                    raise CalledProcessAborted(self.exit_code, self.cmd)
                time.sleep(.1)
        except EOFError as e:
            chan.close()
            raise SSHException(e.message)
        except socket.error as e:
            chan.close()
            raise HostConnectError(self.username, self.hostname, self.exit_code, self.cmd, reason=str(e))
        except:
            chan.close()
            raise
        self.exit_code = chan.recv_exit_status()
        self.logger.debug('%s: "%s" returned %d' % (self.connection_name, self.cmd, self.exit_code))
        if self.exit_code != 0:
            e = self.transport.get_exception()
            if e:
                self.logger.warn('Paramiko exception detected after recv_exit: %s' % e)
                if isinstance(e, socket.error):
                    chan.close()
                    # socket error, raise it as an HostConnectError.
                    raise HostConnectError(self.username, self.hostname, self.exit_code, self.cmd, reason=str(e))
        # Drain any unread data while waiting for end of stream.
        while True:
            s = chan.recv(10240)
            if s:
                self._stdout.write(s)
            elif s == '':
                break
            time.sleep(.1)
        while True:
            s = chan.recv_stderr(10240)
            if s:
                self._stderr.write(s)
            elif s == '':
                break
            time.sleep(.1)
        chan.close()
        if isinstance(self.stdout, Pipe) and str(self.stdout):
            self.logger.debug('%s: stdout: %s' % (self.connection_name, str(self.stdout)))

        if isinstance(self.stderr, Pipe) and str(self.stderr):
            self.logger.debug('%s: stderr: %s' % (self.connection_name, str(self.stderr)))

    def _args_to_shell(self, *args, **kwargs):
        if 'args' in kwargs:
            cmd = kwargs['args']
        else:
            cmd = args[0]
        if not isinstance(cmd, basestring):
            cmd = ' '.join(cmd)
        if 'env' in kwargs and kwargs['env']:
            # Assume that a bash-like shell is available on the
            # other end, and pass environment variables in the command itself.
            env = []
            for key, value in kwargs['env'].iteritems():
                # XXX (mhuang, anupam): For now, because we don't really quote properly, disallow ".
                assert '"' not in value, 'ENV %s: %s value cannot have " in it!' % (key, value)
                env.append('%s="%s"' % (key, value))
            cmd = ' '.join(env) + ' ' + cmd
        return cmd


class SCP(SSHClient):
    def __init__(self, hostname, src, dst, mode, *args, **kwargs):
        super(SCP, self).__init__(hostname, *args, **kwargs)
        self.src = src
        self.dst = dst
        self.mode = mode
        self.scp = None
        # self.cmd is used for description only, SCPClient creates its own cmd.
        self.cmd = 'scp %s %s@%s %s %s' % (self.mode, self.username, self.hostname, self.src, self.dst)

    @property
    def abort(self):
        return self._abort

    @abort.setter
    def abort(self, value):
        if self.scp is not None:
            self.scp._abort = bool(value)
        self._abort = bool(value)

    def _execute(self):
        logger.debug('executing "%s"' % self.cmd)
        self.scp = SCPClient(self.channel, self.cmd)
        getattr(self.scp, self.mode)(self.src, self.dst, recursive=True)
        self.exit_code = 0


class SCPClient(scp.SCPClient):
    # Update scp.SCPClient as follows:
    # - update get/put use channel instead of transport to better support connections cache
    # - update _send_files and _recv_file to support abort
    # - update _send_files and _recv_file to use different progress logging
    def __init__(self, channel, desc):
        super(SCPClient, self).__init__(None, socket_timeout=None)
        self.channel = channel
        self.channel.settimeout(self.socket_timeout)
        self._abort = False
        self.desc = desc

    def put(self, files, remote_path='.', recursive=False, preserve_times=False):
        self.preserve_times = preserve_times
        rcsv = ('', ' -r')[recursive]
        self.channel.exec_command('scp %s -t %s' % (rcsv, self.sanitize(remote_path)))
        self._recv_confirm()
        if not isinstance(files, (list, tuple)):
            files = [files]
        if recursive:
            self._send_recursive(files)
        else:
            self._send_files(files)
        if self.channel:
            self.channel.close()

    def get(self, remote_path, local_path='', recursive=False, preserve_times=False):
        if not isinstance(remote_path, (list, tuple)):
            remote_path = [remote_path]
        remote_path = [self.sanitize(r) for r in remote_path]
        self._recv_dir = local_path or os.getcwd()
        self._rename = len(remote_path) == 1 and not os.path.isdir(local_path)
        if len(remote_path) > 1:
            if not os.path.exists(self._recv_dir):
                msg = 'Local path \'%s\' does not exist' % self._recv_dir
                raise scp.SCPException(msg)
            elif not os.path.isdir(self._recv_dir):
                msg = 'Local path \'%s\' is not a directory' % self._recv_dir
                raise scp.SCPException(msg)
        rcsv = ('', ' -r')[recursive]
        prsv = ('', ' -p')[preserve_times]
        self.channel.exec_command('scp%s%s -f %s' % (rcsv, prsv, ' '.join(remote_path)))
        self._recv_all()
        if self.channel:
            self.channel.close()

    def _send_files(self, files):
        # This method has been updated to break on self.abort and log progress.
        for name in files:
            basename = os.path.basename(name)
            (mode, size, mtime, atime) = self._read_stats(name)
            if self.preserve_times:
                self._send_time(mtime, atime)
            file_hdl = open(name, 'rb')

            # The protocol can't handle \n in the filename.
            # Quote them as the control sequence \^J for now,
            # which is how openssh handles it.
            self.channel.sendall('C%s %d %s\n' %
                                 (mode, size, basename.replace('\n', '\\^J')))
            self._recv_confirm()
            file_pos = 0
            buff_size = self.buff_size
            try:
                last = -1
                while file_pos < size:
                    self.channel.sendall(file_hdl.read(buff_size))
                    file_pos = file_hdl.tell()
                    complete = int((float(file_pos) / float(size)) * 100)
                    if complete % 10 == 0 and last != complete:
                        logger.debug('%s %d%% complete' % (name, complete))
                        last = complete
                    if self._abort:
                        raise CalledProcessAborted(-1, self.desc)
            except:
                self.channel.close()
                raise
            else:
                self.channel.sendall('\x00')
                self._recv_confirm()
            finally:
                file_hdl.close()

    def _recv_file(self, cmd):
        # This method has been updated to break on self.abort and log progress.
        parts = cmd.strip().split(' ', 2)
        try:
            mode = int(parts[0], 8)
            size = int(parts[1])
            path = os.path.join(self._recv_dir, parts[2])
            if self._rename:
                path = self._recv_dir
                self._rename = False
        except Exception as e:
            self.channel.send('\x01')
            self.channel.close()
            logger.debug('Bad file format (%s)' % e)
            return
        try:
            file_hdl = open(path, 'wb')
        except IOError as e:
            self.channel.send(b'\x01' + str(e).encode())
            self.channel.close()
            raise

        buff_size = self.buff_size
        pos = 0
        self.channel.send(b'\x00')
        try:
            last = -1
            while pos < size:
                # we have to make sure we don't read the final byte
                if size - pos <= buff_size:
                    buff_size = size - pos
                file_hdl.write(self.channel.recv(buff_size))
                pos = file_hdl.tell()
                complete = int((float(pos) / float(size)) * 100)
                if complete % 10 == 0 and last != complete:
                    logger.debug('%s %d%% complete' % (path, complete))
                    last = complete
                if self._abort:
                    raise CalledProcessAborted(-1, self.desc)

            msg = self.channel.recv(512)
            if msg and msg[0:1] != b'\x00':
                raise scp.SCPException(msg[1:])
        except socket.timeout:
            raise scp.SCPException('Error receiving, socket.timeout')

        file_hdl.truncate()
        try:
            os.utime(path, self._utime)
            self._utime = None
            os.chmod(path, mode)
            # should we notify the other end?
        finally:
            file_hdl.close()
        # '\x00' confirmation sent in _recv_all


class DummyStdin(object):
    ''' Dummy class to handle writing to stdin '''
    def __init__(self, ssh):
        self.ssh = ssh

    def write(self, data):
        self.ssh.stdin.put(data)

    def close(self):
        pass

if __name__ == '__main__':
    # This supports invoking check_output (with some restrictions) from the command line.
    #
    # Example
    # python ./ssh.py 192.168.1.76 ls -1 /
    #
    # kwargs may also be passed
    # python ./ssh.py 192.168.1.76 ls -1 / username=root password=root
    #
    # kwargs with values starting with "subprocess" are parsed and pass to Popen
    # python ./ssh.py -v 192.168.1.76 ls -1 / stderr=subprocess.STDOUT
    import argparse
    parser = argparse.ArgumentParser(description='run check_output against a host')
    parser.add_argument('-v', '--verbose', action='store_true', help='Enable debug logging')
    parser.add_argument('hostname', help='the hostname')
    parser.add_argument('args', nargs=argparse.REMAINDER, help='args and kwargs')
    args = parser.parse_args()
    level = logging.INFO
    if args.verbose:
        level = logging.DEBUG
    logging.basicConfig(level=level, format='%(message)s')
    cmd = []
    kwargs = {}
    for a in args.args:
        if a[0] != '-' and '=' in a:
            k, v = a.split('=')
            if v.startswith('subprocess.'):
                v = getattr(subprocess, v.split('.')[-1])
            kwargs[k] = v
        else:
            cmd.append(a)
    try:
        logging.debug('cmd: %s kwargs: %s' % (cmd, kwargs))
        logging.info(check_output(args.hostname, cmd, **kwargs))
    except subprocess.CalledProcessError as e:
        if e.output:
            logging.info(e.output)
        sys.exit(1)
    sys.exit(0)
