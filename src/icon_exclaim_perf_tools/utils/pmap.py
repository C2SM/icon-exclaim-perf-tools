import signal
import textwrap
import traceback
import typing
from typing import Callable

import multiprocessing
import time
import os
import sys
from multiprocessing.pool import Pool

from icon_exclaim_perf_tools.utils.signal import replaced_signal_handlers


class RemoteException:
    def __init__(self, ex: Exception):
        self.ex = ex
        self.ex_str = "".join(traceback.format_exception(ex))

class SubProcessExecutor:
    def __init__(self, func, sigterm_received, expand_args: bool):
        self.func = func
        self.sigterm_received = sigterm_received
        self.expand_args = expand_args
    def __call__(self, arg):
        if self.sigterm_received.value:
            return False, RemoteException(Exception("Process was never scheduled due to SIGTERM."))
        try:
            if self.expand_args:
                result = self.func(*arg)
            else:
                result = self.func(arg)
        except Exception as ex:
            return False, RemoteException(ex)
        return True, result

def pmap(
    func: Callable,
    iterable: typing.Iterable[typing.Any],
    expand_args: bool = False,
    ignore_errors: bool = False
):
    manager = multiprocessing.Manager()
    sigterm_recieved = manager.Value('b', False)

    def forward_signal(signal):
        def handler(*args):
            #print(f"forward: {signal}", flush=True)
            if signal in [signal.SIGTERM, signal.SIGINT]:
                sigterm_recieved.value = True
        return handler

    with Pool(9) as p:
        def terminate_pool(*args):
            for child_process in multiprocessing.active_children():
                child_process.terminate()

            for i in range(10):
                is_alive = False
                for child_process in multiprocessing.active_children():
                    is_alive |= child_process.is_alive()
                if not is_alive:
                    break
                time.sleep(1)
            else:
                print("Killing pool. This is fatal.", flush=True)
                for child_process in multiprocessing.active_children():
                    child_process.kill()
                own_pid = os.getpid()
                os.kill(own_pid, signal.SIGKILL)
                sys.exit(1)

        with replaced_signal_handlers(forward_signal):
            task = p.map_async(SubProcessExecutor(func, sigterm_recieved, expand_args), iterable)
            async_result = task.get()

        successful = True
        results, exceptions = [], []
        for arg, (subprocess_successful, result) in zip(iterable, async_result):
            successful &= subprocess_successful
            if subprocess_successful:
                results.append(result)
            else:
                exceptions.append((arg, result))

        if not successful and not ignore_errors:
            msg = "Child process execution failed."
            for _, exception in exceptions:
                msg += "\n"
                msg += textwrap.indent(exception.ex_str, "  ")
            raise RuntimeError(msg)

    if sigterm_recieved.value:
        terminate_pool()

    if ignore_errors:
        return results, exceptions
    return results