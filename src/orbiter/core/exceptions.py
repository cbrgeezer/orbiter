class OrbiterError(Exception):
    """Base class for all Orbiter errors."""


class DAGValidationError(OrbiterError):
    """Raised when a DAG has cycles, missing deps, or duplicate task ids."""


class TaskNotFoundError(OrbiterError):
    pass


class DeadLetterError(OrbiterError):
    """Raised when a task exhausts its retry budget."""


class TransientTaskError(OrbiterError):
    """User code may raise this to explicitly request a retry."""


class PermanentTaskError(OrbiterError):
    """User code may raise this to bypass retries and go straight to failed."""
