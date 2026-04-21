"""Importer entry points.

Submodules are intentionally imported lazily to avoid importer/exporter cycles
during package initialization.
"""

__all__ = [
    "import_cgmes",
    "import_matpower",
    "import_opendss",
    "import_pandapower",
    "import_powermodels",
    "import_powermodels_distribution",
    "import_powersystems",
    "import_pypower",
    "import_pypsa",
    "import_simbench",
]


def __getattr__(name: str):
    if name == "import_cgmes":
        from .cgmes import import_cgmes

        return import_cgmes
    if name == "import_matpower":
        from .matpower import import_matpower

        return import_matpower
    if name == "import_opendss":
        from .opendss import import_opendss

        return import_opendss
    if name == "import_pandapower":
        from .pandapower_json import import_pandapower

        return import_pandapower
    if name == "import_powermodels":
        from .powermodels import import_powermodels

        return import_powermodels
    if name == "import_powermodels_distribution":
        from .powermodels_distribution import import_powermodels_distribution

        return import_powermodels_distribution
    if name == "import_powersystems":
        from .powersystems import import_powersystems

        return import_powersystems
    if name == "import_pypower":
        from .pypower import import_pypower

        return import_pypower
    if name == "import_pypsa":
        from .pypsa import import_pypsa

        return import_pypsa
    if name == "import_simbench":
        from .simbench import import_simbench

        return import_simbench
    raise AttributeError(name)
