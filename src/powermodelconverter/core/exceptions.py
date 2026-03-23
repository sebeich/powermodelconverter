class ConversionError(RuntimeError):
    """Raised when a model cannot be converted safely."""


class ValidationError(RuntimeError):
    """Raised when solver-level validation fails."""
