from __future__ import annotations

from dataclasses import dataclass, field
from importlib import import_module
from typing import Any, Callable

from powermodelconverter.core.route_catalog import ROUTE_CATALOG_BY_TARGET


RouteCallable = Callable[..., Any]


@dataclass(slots=True)
class Route:
    source_tool: str
    target_tool: str
    model_type: str
    importer: RouteCallable | None = None
    exporter: RouteCallable | None = None
    test_cases: list[str] = field(default_factory=list)
    validator: RouteCallable | None = None
    tolerances: dict[str, Any] = field(default_factory=dict)
    notes: str = ""
    requires: list[str] = field(default_factory=list)

    @property
    def route_id(self) -> str:
        case_fragment = ",".join(self.test_cases) if self.test_cases else "*"
        return f"{self.source_tool}:{self.target_tool}:{self.model_type}:{case_fragment}"


ROUTE_REGISTRY: list[Route] = []
_ROUTES_LOADED = False


def register_route(route: Route) -> None:
    if not any(existing.route_id == route.route_id for existing in ROUTE_REGISTRY):
        ROUTE_REGISTRY.append(route)


def register_target_routes(
    *,
    target_tool: str,
    exporter: RouteCallable,
    tolerances: dict[str, Any] | None = None,
    validator: RouteCallable | None = None,
) -> None:
    for item in ROUTE_CATALOG_BY_TARGET.get(target_tool, []):
        register_route(
            Route(
                source_tool=str(item["source_tool"]),
                target_tool=target_tool,
                model_type=str(item["model_type"]),
                importer=_resolve_importer(str(item["source_tool"])),
                exporter=exporter,
                test_cases=[str(item["case_id"])],
                validator=validator,
                tolerances=dict(tolerances or {}),
                notes=str(item.get("notes", "")),
                requires=list(item.get("requires", [])),
            )
        )


def clear_routes() -> None:
    ROUTE_REGISTRY.clear()


def get_routes(
    source: str | None = None,
    target: str | None = None,
    model_type: str | None = None,
    requires: str | None = None,
    requires_julia: bool | None = None,
) -> list[Route]:
    ensure_routes_loaded()
    routes = ROUTE_REGISTRY
    if source is not None:
        routes = [route for route in routes if route.source_tool == source]
    if target is not None:
        routes = [route for route in routes if route.target_tool == target]
    if model_type is not None:
        routes = [route for route in routes if route.model_type == model_type]
    if requires is not None:
        routes = [route for route in routes if requires in route.requires]
    if requires_julia is not None:
        if requires_julia:
            routes = [route for route in routes if any(req.startswith("julia") for req in route.requires)]
        else:
            routes = [route for route in routes if not any(req.startswith("julia") for req in route.requires)]
    return routes


def ensure_routes_loaded() -> None:
    global _ROUTES_LOADED
    if _ROUTES_LOADED:
        return
    _ROUTES_LOADED = True
    for module_name in (
        "powermodelconverter.exporters.matpower",
        "powermodelconverter.exporters.opendss",
        "powermodelconverter.exporters.pandapower_json",
        "powermodelconverter.exporters.pandapower_split",
        "powermodelconverter.exporters.powermodels",
        "powermodelconverter.exporters.powermodels_distribution",
        "powermodelconverter.exporters.powersystems",
        "powermodelconverter.exporters.pypsa",
        "powermodelconverter.exporters.cgmes",
    ):
        import_module(module_name)


def _resolve_importer(source_tool: str) -> RouteCallable | None:
    module_name, attr_name = {
        "cgmes": ("powermodelconverter.importers.cgmes", "import_cgmes"),
        "matpower": ("powermodelconverter.importers.matpower", "import_matpower"),
        "opendss": ("powermodelconverter.importers.opendss", "import_opendss"),
        "pandapower": ("powermodelconverter.importers.pandapower_json", "import_pandapower"),
        "powermodelsdistribution": ("powermodelconverter.importers.powermodels_distribution", "import_powermodels_distribution"),
        "powersystems": ("powermodelconverter.importers.powersystems", "import_powersystems"),
        "pypower": ("powermodelconverter.importers.pypower", "import_pypower"),
        "pypsa": ("powermodelconverter.importers.pypsa", "import_pypsa"),
        "simbench": ("powermodelconverter.importers.simbench", "import_simbench"),
    }.get(source_tool, (None, None))
    if module_name is None or attr_name is None:
        return None
    module = import_module(module_name)
    return getattr(module, attr_name)


ensure_routes_loaded()
