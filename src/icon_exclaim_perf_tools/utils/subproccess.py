import dataclasses
import pty
import signal
import textwrap
import typing
from typing import Optional, Callable

import os
import sys
import io
import subprocess
import selectors
import shlex

from icon_exclaim_perf_tools.utils.signal import replaced_signal_handlers

@dataclasses.dataclass
class DummyProcess:
    pid: int
    returncode: int
    stdout: typing.Any
    stderr: typing.Any
    fd: typing.Any

    _clean: bool = False

    def cleanup(self):
        if not self._clean:
            self._clean = True
            self.stdout.close()
            self.stderr.close()

    def poll(self):
        finished, status = os.waitpid(self.pid, os.WNOHANG)
        if finished:
            self.cleanup()
            self.returncode = os.WEXITSTATUS(status)
            return self.returncode
        return None

    def send_signal(self, signal):
        if signal is signal.SIGINT:
            os.write(self.fd, '\x03'.encode())
        else:
            os.killpg(self.pid, signal)


def popen_pty(cmd, cwd, *args, **kwargs):
    stdout_pipe_read, stdout_pipe_write = os.pipe()
    stderr_pipe_read, stderr_pipe_write = os.pipe()

    # Fork the current process
    pid, fd = pty.fork()

    if pid == 0:  # Child process
        os.close(stdout_pipe_read)
        os.close(stderr_pipe_read)
        stdout = os.fdopen(stdout_pipe_write, 'w')
        stderr = os.fdopen(stderr_pipe_write, 'w')
        os.dup2(stdout.fileno(), sys.stdout.fileno())
        os.dup2(stderr.fileno(), sys.stderr.fileno())

        args = shlex.split(cmd)
        os.chdir(cwd)
        os.execvp(args[0], args)
    else:
        os.close(stdout_pipe_write)
        os.close(stderr_pipe_write)
        stdout = os.fdopen(stdout_pipe_read, "rb")
        stderr = os.fdopen(stderr_pipe_read, "rb")
        return DummyProcess(pid=pid, fd=fd, returncode=None, stdout=stdout, stderr=stderr)


_cwd = os.getcwd()
def execute_command(
    cmd: str,
    *,
    strip=True,
    combine_output=False,
    fail_on_stderr_output=False,
    include_output_in_error=True,
    cb: Optional[Callable] = None,
    cwd: Optional[str] = None,
    signal_handler: Optional[Callable] = None,
):
    # make sure the subprocess doesn't inherit the signals we are using
    #with replaced_signal_handlers(lambda _: signal.SIG_DFL, set(signal.Signals) - {signal.SIGKILL, signal.SIGSTOP}):
    with replaced_signal_handlers(lambda _: signal.SIG_DFL):
        process = subprocess.Popen(
            cmd,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            bufsize=0,  # don't buffer so that we immediately get the output
            cwd=cwd,
            #preexec_fn=os.setsid,
            #restore_signals=False
        )
    #process = popen_pty(cmd, cwd=cwd)

    def forward_signal(signal):
        def handler(*args):
            #print(f"got signal in child: {signal}", flush=True)
            process.send_signal(signal)
            if signal_handler:
                signal_handler(*args)

        return handler

    with replaced_signal_handlers(forward_signal):
        stdout_io = io.StringIO()
        stderr_io = stdout_io if combine_output else io.StringIO()

        sel = selectors.DefaultSelector()
        sel.register(process.stdout, selectors.EVENT_READ)
        sel.register(process.stderr, selectors.EVENT_READ)

        while process.poll() is None:
            for key, _ in sel.select():  # todo: timeout makes a busy loop?
                line = key.fileobj.readline().decode()
                if line:
                    if cb:
                        cb(line[:-1])
                    io_stream = stdout_io if key.fileobj is process.stdout else stderr_io
                    io_stream.write(line)

        stdout_io.seek(0), stderr_io.seek(0)
        stdout, stderr = stdout_io.read(), stderr_io.read()

        if strip:
            stdout, stderr = stdout.strip(), stderr.strip()

    if process.returncode != 0 or (fail_on_stderr_output and stderr):
        msg = f"Executing command `{cmd}` failed with exit code `{process.returncode}` (cwd: {cwd})"
        if include_output_in_error:
            msg += (f"\n"
                    f"Stdout:\n"
                    f"{textwrap.indent(stdout, ' ')}\n"
                    f"Stderr:\n"
                    f"{textwrap.indent(stderr, ' ')}")
        raise RuntimeError(msg)

    if combine_output or fail_on_stderr_output:
        return stdout
    else:
        return stdout, stderr