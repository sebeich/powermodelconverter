"""Core models and lazy access to registry helpers."""

from .model import CanonicalCase, ComplexVoltageProfile

__all__ = [
    "CanonicalCase",
    "ComplexVoltageProfile",
    "ROUTE_REGISTRY",
    "Route",
    "get_routes",
    "register_route",
]


def __getattr__(name: str):
    if name in {"ROUTE_REGISTRY", "Route", "get_routes", "register_route"}:
        from . import registry as _registry

        return getattr(_registry, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
