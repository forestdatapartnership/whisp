import logging
import sys

BASE_MSG_FORMAT = (
    "[%(filename)s | %(funcName)s() | l.%(lineno)s] %(levelname)s: %(message)s"
)


class StdoutLogger:
    def __init__(self, name: str, msg_format: str = BASE_MSG_FORMAT) -> None:
        self.handler = logging.StreamHandler(sys.stdout)
        self.handler.setFormatter(logging.Formatter(msg_format))
        self.handler.setLevel(logging.DEBUG)
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
