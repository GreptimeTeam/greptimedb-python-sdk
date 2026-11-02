"""Public error type shared by both backends."""


class GreptimeError(Exception):
    def __init__(self, message, retriable=False):
        super().__init__(message)
        self.retriable = retriable
