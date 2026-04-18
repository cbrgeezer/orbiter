class HoynatskiError(Exception):
    """Base class for all Hoynatski errors."""


class DAGValidationError(HoynatskiError):
    """Raised when a DAG has cycles, missing deps, or duplicate task ids."""


class TaskNotFoundError(HoynatskiError):
    pass


class DeadLetterError(HoynatskiError):
    """Raised when a task exhausts its retry budget."""


class TransientTaskError(HoynatskiError):
    """User code may raise this to explicitly request a retry."""


class PermanentTaskError(HoynatskiError):
    """User code may raise this to bypass retries and go straight to failed."""
