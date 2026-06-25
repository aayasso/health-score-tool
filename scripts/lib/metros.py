"""
Centralized metro configuration loader.
Single source of truth: config/metros.yml.
"""

import os
import yaml
from pathlib import Path
from typing import Dict, List

_CONFIG_PATH = Path(__file__).resolve().parent.parent.parent / "config" / "metros.yml"


def _load() -> list:
    with open(_CONFIG_PATH) as f:
        return yaml.safe_load(f)["metros"]


_METROS = _load()


def all_metros() -> List[str]:
    """All 8 metro names."""
    return [m["name"] for m in _METROS]


def pilot_metros() -> List[str]:
    """In-scope metros (Pittsburgh, LA, Phoenix, Charlotte)."""
    return [m["name"] for m in _METROS if m["scope"] == "pilot"]


def expansion_metros() -> List[str]:
    """Expansion metros (Chicago, Houston, Atlanta, Denver)."""
    return [m["name"] for m in _METROS if m["scope"] == "expansion"]


def metro_labels() -> Dict[str, str]:
    """Dict mapping metro name → display label (currently identity)."""
    return {m["name"]: m["name"] for m in _METROS}


def state_fips() -> Dict[str, str]:
    """Dict mapping state abbreviation → FIPS code."""
    return {m["state"]: m["state_fips"] for m in _METROS}


def state_metro_map() -> Dict[str, List[str]]:
    """Dict mapping state abbreviation → list of metro names in that state."""
    result: Dict[str, List[str]] = {}
    for m in _METROS:
        result.setdefault(m["state"], []).append(m["name"])
    return result


def noise_raster_map(drive_prefix: str) -> Dict[str, str]:
    """Dict mapping state abbreviation → full noise raster path."""
    return {m["state"]: f"{drive_prefix}/{m['noise_raster_filename']}" for m in _METROS}
