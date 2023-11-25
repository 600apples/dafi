import signal
import atexit

SIGNALS_TO_NAMES_DICT = dict(
    (getattr(signal, n), n) for n in dir(signal) if n.startswith("SIG") and "_" not in n
)


def set_signal_handler(handler):
    atexit.register(handler)
    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)
