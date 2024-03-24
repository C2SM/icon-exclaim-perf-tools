import signal
import contextlib

@contextlib.contextmanager
def replaced_signal_handlers(new_handler_factory, signals=None):
    if not signals:
        signals = set(signal.Signals) - {signal.SIGKILL, signal.SIGSTOP, signal.SIGCLD}
    saved_signal_handlers = {}
    for sig in signals:
        saved_signal_handlers[sig] = signal.signal(sig, new_handler_factory(sig))

    try:
        yield
    finally:
        for sig, orig_handler in saved_signal_handlers.items():
            signal.signal(sig, orig_handler)

def replace_signal_handlers(new_handler_factory, signals=None):
    if not signals:
        signals = set(signal.Signals) - {signal.SIGKILL, signal.SIGSTOP, signal.SIGCLD}
    saved_signal_handlers = {}
    for sig in signals:
        saved_signal_handlers[sig] = signal.signal(sig, new_handler_factory(sig))
    return saved_signal_handlers

def restore_signal_handlers(saved_signal_handlers):
    for sig, orig_handler in saved_signal_handlers.items():
        signal.signal(sig, orig_handler)

def block_signals(sigset = { signal.SIGINT }):
    mask = signal.pthread_sigmask(signal.SIG_BLOCK, {})
    signal.pthread_sigmask(signal.SIG_BLOCK, sigset)
    return mask

def restore_signals(mask):
    signal.pthread_sigmask(signal.SIG_SETMASK, mask)