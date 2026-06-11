#!/usr/bin/env python3
"""Retrieve PyPSA-Eur OSM raw electricity JSONs for Germany in state chunks."""

from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path

import requests


FEATURES = {
    "cables_way": [
        'way["power"="cable"]',
        'way["construction:power"="cable"]',
        'way["power"="construction"]["construction"="cable"]',
    ],
    "lines_way": [
        'way["power"="line"]',
        'way["construction:power"="line"]',
        'way["power"="construction"]["construction"="line"]',
    ],
    "routes_relation": [
        'relation["route"="power"]',
        'relation["power"="circuit"]',
        'relation["construction:power"="line"]',
        'relation["construction:power"="cable"]',
        'relation["power"="construction"]',
    ],
    "substations_way": [
        'way["power"="substation"]',
        'way["construction:power"="substation"]',
        'way["power"="construction"]["construction"="substation"]',
    ],
    "substations_relation": [
        'relation["power"="substation"]',
        'relation["construction:power"="substation"]',
        'relation["power"="construction"]["construction"="substation"]',
    ],
}

STATE_CODES = [
    "DE-BW",
    "DE-BY",
    "DE-BE",
    "DE-BB",
    "DE-HB",
    "DE-HH",
    "DE-HE",
    "DE-MV",
    "DE-NI",
    "DE-NW",
    "DE-RP",
    "DE-SL",
    "DE-SN",
    "DE-ST",
    "DE-SH",
    "DE-TH",
]


def overpass_query(state: str, selectors: list[str], timeout: int) -> str:
    selectors_block = " ".join(f"{selector}(area.searchArea);" for selector in selectors)
    return f"""
        [out:json][timeout:{timeout}];
        area["ISO3166-2"="{state}"]->.searchArea;
        (
        {selectors_block}
        );
        out body geom;
    """


def request_json(
    session: requests.Session,
    url: str,
    query: str,
    user_agent: str,
    request_timeout: int,
    attempts: int,
) -> dict:
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            response = session.post(
                url,
                data=query,
                headers={"User-Agent": user_agent},
                timeout=request_timeout,
            )
            response.raise_for_status()
            return response.json()
        except (json.JSONDecodeError, requests.RequestException) as exc:
            last_error = exc
            if attempt == attempts:
                break
            sleep_s = 10 + attempt * 20
            print(f"    retry {attempt}/{attempts} after {exc}; sleeping {sleep_s}s", flush=True)
            time.sleep(sleep_s)
    raise RuntimeError(f"Overpass request failed after {attempts} attempts: {last_error}")


def merge_elements(chunks: list[dict]) -> dict:
    merged: dict[tuple[str, int], dict] = {}
    for chunk in chunks:
        for element in chunk.get("elements", []):
            element_type = element.get("type")
            element_id = element.get("id")
            if element_type is None or element_id is None:
                continue
            merged[(str(element_type), int(element_id))] = element
    return {
        "version": 0.6,
        "generator": "powermodelconverter chunked Overpass retrieval for PyPSA-Eur",
        "elements": list(merged.values()),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--url", default="https://overpass-api.de/api/interpreter")
    parser.add_argument("--timeout", type=int, default=360)
    parser.add_argument("--request-timeout", type=int, default=420)
    parser.add_argument("--attempts", type=int, default=3)
    parser.add_argument("--sleep", type=float, default=1.0)
    parser.add_argument(
        "--user-agent",
        default="PowerModelConverter PyPSA-Eur OSM raw DE chunked retrieval",
    )
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    with requests.Session() as session:
        for feature, selectors in FEATURES.items():
            chunks: list[dict] = []
            print(f"{feature}", flush=True)
            for state in STATE_CODES:
                print(f"  {state}", flush=True)
                data = request_json(
                    session,
                    args.url,
                    overpass_query(state, selectors, args.timeout),
                    args.user_agent,
                    args.request_timeout,
                    args.attempts,
                )
                print(f"    elements={len(data.get('elements', []))}", flush=True)
                chunks.append(data)
                time.sleep(args.sleep)

            output = merge_elements(chunks)
            output_path = output_dir / f"{feature}.json"
            tmp_path = output_path.with_suffix(".json.tmp")
            with tmp_path.open("w") as fh:
                json.dump(output, fh, indent=2)
            os.replace(tmp_path, output_path)
            print(f"wrote {output_path} elements={len(output['elements'])}", flush=True)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
