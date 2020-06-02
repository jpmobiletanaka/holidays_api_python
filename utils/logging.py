import typing as t
import logging
import datetime as dt
import functools
from contextlib import contextmanager


__author__ = "Pavel Frolov"
__email__ = "pavelfk3@gmail.com"


class logex:
    """
    @logex(logger, "Cannot run function")
    def func()
        pass

    with logex(logger, "Cannot ..."):
        pass

    """
    __slots__ = ('logger', 'error_message')

    def __init__(self, logger: logging.Logger, error_message: str = None):
        self.logger = logger
        self.error_message = error_message
        """Understandable context of error"""

    def __call__(self, func):
        @functools.wraps(func)
        def inner(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if self.error_message:
                    self.logger.error(self.error_message)
                self.logger.exception(e)
                raise e
        return inner

    def __enter__(self):
        pass

    def __exit__(self, et, ev, tb):
        if not ev:
            return
        if self.error_message:
            self.logger.error(self.error_message)
        self.logger.exception(ev)


scalar_types = (int, float, complex, bool, str, bytes)


class logerror:
    """
    Logs error msg if error happens
    does not log exception

    @logerror(logger, "Cannot run function: %s", arg)
    def func()
        pass

    with logerror(logger, "Cannot ... %s", arg):
        pass
    """
    __slots__ = ('logger', 'error_message', 'message_args')

    def __init__(self, logger: logging.Logger, error_message: str, *args):
        self.logger = logger
        self.error_message = error_message
        """Understandable context of error"""
        self.message_args = args
        """Args for logger message"""

    def __call__(self, func):
        @functools.wraps(func)
        def inner(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception:
                error_context = (
                    '\n[arguments(all count): only scalars]\n'
                    'args(%s): %s\n'
                    'kwargs(%s): %s'
                ) % (
                    len(args), [a for a in args if isinstance(a, scalar_types)],
                    len(kwargs), {k: v for k, v in kwargs.items() if isinstance(v, scalar_types)}
                )
                self.logger.error(self.error_message + error_context, *self.message_args)
                raise
        return inner

    def __enter__(self):
        pass

    def __exit__(self, et, ev, tb):
        if not ev:
            return
        self.logger.error(self.error_message, *self.message_args)


class logtime:
    """
    Log running time

    @logtime(logger, "Loading data")
    def func()
        pass

    @logtime(logger, 'Loading input', debug=False, ignore_sec=5, warn_sec=30)
    def func(self):
        pass

    with logtime(logger, "Loading data"):
        pass
    """
    __slots__ = ('logger', 'message', 'start_time', 'is_debug', 'ignore_sec', 'warn_sec')

    # todo: improve by: def logtime(message: str, *args, debug=True, ignore_sec=None):
    def __init__(self, logger: logging.Logger, message: str, debug=True,
                 ignore_sec: t.Optional[t.Union[int, float]] = None, warn_sec: t.Optional[t.Union[int, float]] = None):
        """
        :param logger:
        :param message: Human readable message
        :param debug: enable/disable start debug message
        :param ignore_sec: no finish message if running time less than the parameter
        :param warn_sec: warn message if running time more than the parameter
        """
        self.logger = logger
        self.message = message
        """Understandable context of running code"""
        self.is_debug = debug
        self.ignore_sec = ignore_sec
        self.warn_sec = warn_sec

    def __call__(self, func):
        @functools.wraps(func)
        def inner(*args, **kwargs):
            start_time = dt.datetime.now()
            if self.is_debug:
                self.logger.debug('t> ' + self.message)

            result = func(*args, **kwargs)

            delta = dt.datetime.now() - start_time
            total_sec = delta.total_seconds()
            if self.warn_sec and total_sec > self.warn_sec:
                self.logger.warning('t| %s: warning time limit(%s) exceeded: %s', self.message, self.warn_sec, delta)
            if self.ignore_sec and total_sec < self.ignore_sec:
                return result
            self.logger.info('t< %s: running time: %s',
                             self.message, delta)
            return result
        return inner

    def __enter__(self):
        self.start_time = dt.datetime.now()
        if self.is_debug:
            self.logger.debug('t> ' + self.message)

    def __exit__(self, et, ev, tb):
        if ev:
            self.logger.error('t< Error: %s', self.message)

        delta = dt.datetime.now() - self.start_time
        total_sec = delta.total_seconds()
        if self.warn_sec and total_sec > self.warn_sec:
            self.logger.warning('t| %s: warning time limit(%s) exceeded: %s', self.message, self.warn_sec, delta)
        if self.ignore_sec and total_sec < self.ignore_sec:
            return

        self.logger.info('t< %s: running time: %s',
                         self.message, dt.datetime.now() - self.start_time)


@contextmanager
def log_level(name: str, level: int):
    """

    with log_level('dataengine', logging.WARNING):
        do()

    :param name: logger name
    :param level: logging level
    """
    if name not in logging.root.manager.loggerDict:
        yield
        return

    logger = logging.getLogger(name)
    old_level = logger.level
    logger.setLevel(level)
    try:
        yield
    finally:
        logger.setLevel(old_level)
