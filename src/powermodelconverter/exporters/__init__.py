"""Exporter entry points.

Submodules are intentionally imported lazily to avoid circular imports during
package initialization.
"""

__all__ = [
    "export_cgmes",
    "export_matpower",
    "export_opendss",
    "export_pandapower",
    "export_pandapower_split",
    "export_powermodels",
    "export_powermodels_distribution",
    "export_powersystems",
    "export_pypower",
    "export_pypsa",
]


def __getattr__(name: str):
    if name == "export_cgmes":
        from .cgmes import export_cgmes

        return export_cgmes
    if name == "export_matpower":
        from .matpower import export_matpower

        return export_matpower
    if name == "export_opendss":
        from .opendss import export_opendss

        return export_opendss
    if name == "export_pandapower":
        from .pandapower_json import export_pandapower

        return export_pandapower
    if name == "export_pandapower_split":
        from .pandapower_split import export_pandapower_split

        return export_pandapower_split
    if name == "export_powermodels":
        from .powermodels import export_powermodels

        return export_powermodels
    if name == "export_powermodels_distribution":
        from .powermodels_distribution import export_powermodels_distribution

        return export_powermodels_distribution
    if name == "export_powersystems":
        from .powersystems import export_powersystems

        return export_powersystems
    if name == "export_pypower":
        from .pypower import export_pypower

        return export_pypower
    if name == "export_pypsa":
        from .pypsa import export_pypsa

        return export_pypsa
    raise AttributeError(name)
