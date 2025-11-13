import logging
import sys

BASE_MSG_FORMAT = (
    "[%(filename)s | %(funcName)s() | l.%(lineno)s] %(levelname)s: %(message)s"
)


class StdoutLogger:
    def __init__(self, name: str, msg_format: str = BASE_MSG_FORMAT) -> None:
        # Create handler that auto-flushes for Colab/notebook visibility
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter(msg_format))
        handler.setLevel(logging.DEBUG)

        # Override emit to force flush after each message
        original_emit = handler.emit

        def emit_with_flush(record):
            original_emit(record)
            sys.stdout.flush()

        handler.emit = emit_with_flush

        self.handler = handler
        self.logger = logging.getLogger(name)
        self.logger.addHandler(self.handler)
        self.logger.propagate = False

    # Add missing methods that delegate to the internal logger
    def debug(self, message):
        self.logger.debug(message)

    def info(self, message):
        self.logger.info(message)

    def warning(self, message):
        self.logger.warning(message)

    def error(self, message):
        self.logger.error(message)

    def critical(self, message):
        self.logger.critical(message)

    def setLevel(self, level):
        self.logger.setLevel(level)

    @property
    def level(self):
        """Return the logger's effective level."""
        return self.logger.level

    def hasHandlers(self):
        """Check if the logger has any handlers."""
        return self.logger.hasHandlers()

    def addHandler(self, handler):
        """Add a handler to the logger."""
        self.logger.addHandler(handler)


class FileLogger:
    def __init__(
        self,
        log_filepath: str,
        msg_format: str = BASE_MSG_FORMAT,
        log_to_stdout: bool = True,
    ) -> None:
        self.handler = logging.FileHandler(log_filepath)
        self.handler.setFormatter(logging.Formatter(msg_format))
        self.handler.setLevel(logging.DEBUG)
        self.logger = logging.getLogger(f"{__name__}.file_logger_{log_filepath}")
        self.logger.addHandler(self.handler)
        self.logger.propagate = False

        if log_to_stdout:
            self.stdout_handler = logging.StreamHandler(sys.stdout)
            self.stdout_handler.setFormatter(logging.Formatter(msg_format))
            self.stdout_handler.setLevel(logging.DEBUG)
            self.logger.addHandler(self.stdout_handler)

    # Add missing methods for FileLogger too
    def debug(self, message):
        self.logger.debug(message)

    def info(self, message):
        self.logger.info(message)

    def warning(self, message):
        self.logger.warning(message)

    def error(self, message):
        self.logger.error(message)

    def critical(self, message):
        self.logger.critical(message)

    def setLevel(self, level):
        self.logger.setLevel(level)

    @property
    def level(self):
        """Return the logger's effective level."""
        return self.logger.level

    def hasHandlers(self):
        """Check if the logger has any handlers."""
        return self.logger.hasHandlers()

    def addHandler(self, handler):
        """Add a handler to the logger."""
        self.logger.addHandler(handler)
