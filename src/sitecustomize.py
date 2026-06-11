from __future__ import annotations

"""Small runtime shims for legacy optional importers.

ding0 0.2.x imports ``pypsa.io.import_series_from_dataframe``, a helper that
was removed from modern PyPSA. pmc itself uses modern PyPSA, so downgrading the
hub dependency would be worse than providing the narrow legacy symbol ding0
needs at import time.
"""

import sys
import types


def _install_numpy_legacy_aliases() -> None:
    try:
        import numpy as np
    except Exception:
        return
    if not hasattr(np, "NaN"):
        np.NaN = np.nan


def _install_pypsa_io_shim() -> None:
    if "pypsa.io" in sys.modules:
        return
    try:
        import pypsa
    except Exception:
        return

    module = types.ModuleType("pypsa.io")

    def import_series_from_dataframe(network, dataframe, component, attr):
        table_name = f"{str(component).lower()}s_t"
        container = getattr(network, table_name, None)
        if container is not None and hasattr(container, "__setitem__"):
            container[attr] = dataframe
            return
        setattr(network, table_name, {attr: dataframe})

    module.import_series_from_dataframe = import_series_from_dataframe
    sys.modules["pypsa.io"] = module
    setattr(pypsa, "io", module)


def _install_pandas_sqlalchemy13_shim() -> None:
    try:
        import pandas as pd
    except Exception:
        return
    original = pd.read_sql_query

    def read_sql_query(sql, con, *args, **kwargs):
        try:
            return original(sql, con, *args, **kwargs)
        except TypeError as exc:
            if "Query must be a string" not in str(exc) or not hasattr(sql, "compile"):
                raise
            index_col = kwargs.pop("index_col", None)
            with con.connect() as conn:
                result = conn.execute(sql)
                rows = result.fetchall()
                columns = list(result.keys())
            frame = pd.DataFrame(rows, columns=columns)
            if index_col is not None and index_col in frame.columns:
                frame = frame.set_index(index_col)
            return frame

    pd.read_sql_query = read_sql_query


_install_numpy_legacy_aliases()
_install_pypsa_io_shim()
_install_pandas_sqlalchemy13_shim()
