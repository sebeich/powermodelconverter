from __future__ import annotations

from configparser import ConfigParser
import os
from pathlib import Path
from typing import Any


def _load_sqlalchemy_text():
    try:
        from sqlalchemy import text

        return text
    except ImportError as exc:
        raise ImportError(
            "ding0 region import requires SQLAlchemy through the ding0 optional extra. "
            "Install it with: pip install -e '.[ding0]'"
        ) from exc


def open_oedb_session() -> tuple[Any, Any]:
    """Open an ego.io OEDB session and return ``(session, engine)``."""

    config_path = Path.home() / ".egoio" / "config.ini"
    _materialize_egoio_config_from_env(config_path)
    parser = ConfigParser()
    if not config_path.exists() or not parser.read(config_path):
        raise RuntimeError(
            "OEDB credentials were not found. ding0 reads them from ~/.egoio/config.ini; "
            "create that file or fill .env.ding0 before using --source ags:... or mvgd:... "
            "with a live OEDB run."
        )

    try:
        from egoio.tools import db
    except ImportError as exc:
        raise ImportError(
            "egoio is not installed. Install the optional extra: pip install -e '.[ding0]'"
        ) from exc

    section = os.environ.get("PMC_EGOIO_SECTION", "oep")
    try:
        session = db.connection(filepath=str(config_path), section=section, readonly=False)
    except TypeError:
        session = db.connection(str(config_path), section)

    if hasattr(session, "connect") and hasattr(session, "execute"):
        engine = session
    else:
        engine = getattr(session, "bind", None) or getattr(session, "get_bind", lambda: None)()
    if engine is None:
        raise RuntimeError("egoio opened an OEDB session, but no SQLAlchemy engine was available.")
    return session, engine


def _materialize_egoio_config_from_env(config_path: Path) -> None:
    if config_path.exists():
        return

    explicit_config = os.environ.get("PMC_EGOIO_CONFIG_PATH")
    if explicit_config:
        source = Path(explicit_config).expanduser()
        if source.exists():
            config_path.parent.mkdir(parents=True, exist_ok=True)
            config_path.write_text(source.read_text())
            return

    user = os.environ.get("PMC_OEDB_USER") or os.environ.get("OEDB_USER")
    password = os.environ.get("PMC_OEDB_PASSWORD") or os.environ.get("OEDB_PASSWORD")
    host = os.environ.get("PMC_OEDB_HOST") or os.environ.get("OEDB_HOST")
    database = os.environ.get("PMC_OEDB_DATABASE") or os.environ.get("OEDB_DATABASE")
    port = os.environ.get("PMC_OEDB_PORT") or os.environ.get("OEDB_PORT") or "5432"
    dialect = os.environ.get("PMC_EGOIO_DIALECT") or os.environ.get("EGOIO_DIALECT") or "oedialect"
    if not all((user, password, host, database)):
        return

    section = os.environ.get("PMC_EGOIO_SECTION", "oep")
    parser = ConfigParser()
    payload = {
        "dialect": dialect,
        "user": user,
        "username": user,
        "pw": password,
        "password": password,
        "host": host,
        "port": port,
        "database": database,
    }
    parser[section] = payload
    parser.setdefault("oep", payload)
    config_path.parent.mkdir(parents=True, exist_ok=True)
    with config_path.open("w") as handle:
        parser.write(handle)


def get_polygon_for_ags(engine: Any, ags: str) -> Any:
    """Resolve a German AGS to a WGS84 polygon using OEDB VG250, then OSM."""

    if not isinstance(ags, str) or not ags.isdigit() or len(ags) != 8:
        raise ValueError(f"AGS must be an eight-digit string, got {ags!r}.")

    text = _load_sqlalchemy_text()
    try:
        from shapely import wkb
    except ImportError as exc:
        raise ImportError("shapely is required for ding0 region import. Install: pip install -e '.[ding0]'") from exc

    query = text(
        """
        SELECT ST_AsBinary(ST_Union(geometry)) AS geom
        FROM boundaries.bkg_vg250_6_gem
        WHERE ags = :ags
        """
    )
    try:
        with engine.connect() as conn:
            row = conn.execute(query, {"ags": ags}).first()
        if row is not None and row[0] is not None:
            return wkb.loads(bytes(row[0]))
    except Exception:
        pass

    return _polygon_from_nominatim(ags)


def _polygon_from_nominatim(ags: str) -> Any:
    try:
        import requests
        from shapely.geometry import shape
    except ImportError as exc:
        raise ImportError(
            "requests and shapely are required for the OSM fallback. Install: pip install -e '.[ding0]'"
        ) from exc

    known_queries = {
        "09462000": "Bayreuth, Bavaria, Germany",
    }
    query = known_queries.get(ags, f"AGS {ags}, Germany")
    response = requests.get(
        "https://nominatim.openstreetmap.org/search",
        params={"q": query, "format": "geojson", "polygon_geojson": 1, "limit": 1},
        headers={"User-Agent": "powermodelconverter ding0 importer"},
        timeout=30,
    )
    response.raise_for_status()
    features = response.json().get("features", [])
    if not features:
        raise RuntimeError(f"Could not resolve AGS {ags} through OEDB VG250 or OSM Nominatim.")
    return shape(features[0]["geometry"])


def find_mv_grid_districts(engine: Any, polygon: Any) -> list[int]:
    """Find MV grid district IDs intersecting a WGS84 polygon."""

    text = _load_sqlalchemy_text()
    wkt_literal = polygon.wkt.replace("'", "''")
    queries = (
        f"""
        SELECT bus_id AS id
        FROM grid.egon_mv_grid_district
        WHERE ST_Intersects(geom, ST_SetSRID(ST_GeomFromText('{wkt_literal}'), 4326))
        ORDER BY bus_id
        """,
        f"""
        SELECT bus_id AS id
        FROM model_draft.ego_grid_mv_griddistrict
        WHERE ST_Intersects(geom, ST_SetSRID(ST_GeomFromText('{wkt_literal}'), 4326))
        ORDER BY bus_id
        """,
    )
    last_error: Exception | None = None
    for sql in queries:
        try:
            with engine.connect() as conn:
                rows = conn.execute(text(sql)).fetchall()
            ids = [int(row[0]) for row in rows]
            if ids:
                return ids
        except Exception as exc:
            last_error = exc
    if last_error is not None:
        ids = _find_mv_grid_districts_via_oep_rows_api(polygon)
        if ids:
            return ids
        raise RuntimeError(f"Could not query OEDB MV grid districts: {last_error}") from last_error
    return _find_mv_grid_districts_via_oep_rows_api(polygon)


def _find_mv_grid_districts_via_oep_rows_api(polygon: Any) -> list[int]:
    try:
        import requests
        from pyproj import Transformer
        from shapely import wkb
        from shapely.ops import transform
    except ImportError as exc:
        raise ImportError(
            "requests, pyproj, and shapely are required for the OEP rows fallback. "
            "Install: pip install -e '.[ding0]'"
        ) from exc

    transformer = Transformer.from_crs("EPSG:4326", "EPSG:3035", always_xy=True)
    polygon_3035 = transform(transformer.transform, polygon)
    response = requests.get(
        "https://openenergy-platform.org/api/v0/schema/model_draft/tables/"
        "ego_grid_mv_griddistrict/rows/",
        params={"limit": 5000},
        timeout=90,
    )
    response.raise_for_status()
    ids: list[int] = []
    for row in response.json():
        raw_geom = row.get("geom")
        raw_id = row.get("subst_id")
        if raw_geom is None or raw_id is None:
            continue
        try:
            geom = wkb.loads(bytes.fromhex(str(raw_geom)))
            if geom.intersects(polygon_3035):
                ids.append(int(raw_id))
        except Exception:
            continue
    return sorted(set(ids))


def fetch_oep_rows(
    schema: str,
    table: str,
    *,
    where: list[str] | None = None,
    columns: list[str] | None = None,
    limit: int = 50_000,
) -> list[dict[str, Any]]:
    """Fetch rows through the public OEP rows API.

    This is intentionally narrow: it is used as a compatibility fallback for
    ding0 0.2.x when oedialect's advanced SQL connection endpoint is not
    available on the live Open Energy Platform.
    """

    try:
        import requests
    except ImportError as exc:
        raise ImportError("requests is required for the OEP rows fallback. Install: pip install -e '.[ding0]'") from exc

    params: list[tuple[str, str | int]] = [("limit", limit)]
    for clause in where or []:
        params.append(("where", clause))
    if columns:
        params.append(("columns", ",".join(columns)))

    response = requests.get(
        f"https://openenergy-platform.org/api/v0/schema/{schema}/tables/{table}/rows/",
        params=params,
        timeout=120,
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, list):
        raise RuntimeError(f"OEP rows API returned unexpected payload for {schema}.{table}: {type(payload)!r}")
    return payload
