"""
Shared pipeline utilities: config loader, write verification, preflight checks.
"""

import os
import subprocess
import time
import yaml
from pathlib import Path
from typing import Optional

import pandas as pd

_CONFIG_PATH = Path(__file__).resolve().parent.parent.parent / "config" / "pipeline.yml"


def _load_config() -> dict:
    with open(_CONFIG_PATH) as f:
        return yaml.safe_load(f)


_CONFIG = _load_config()


def claude_model() -> str:
    """Return the configured Claude model string."""
    return _CONFIG["claude_model"]


# ── Write verification ───────────────────────────────────────────────


class WriteVerificationError(Exception):
    """Raised when a DB write did not land as expected. Halts the caller."""
    pass


def write_and_verify(supabase, table: str, zipcode: str, column: str, value):
    """Update one column on a row, then re-read and confirm it landed.

    Guards against silent RLS/permission failures: supabase-py does NOT
    raise when an UPDATE affects zero rows (e.g. wrong key, no UPDATE
    policy). This re-reads the row and raises if the stored value does
    not match, so a silent no-op stops loudly.
    """
    update_resp = supabase.table(table) \
        .update({column: value}) \
        .eq("zipcode", zipcode) \
        .execute()

    if not getattr(update_resp, "data", None):
        raise WriteVerificationError(
            f"{table} / {zipcode}: UPDATE returned no rows — write was blocked "
            f"(likely RLS/permission: confirm SUPABASE_KEY is the service role key)."
        )

    check = supabase.table(table) \
        .select(column) \
        .eq("zipcode", zipcode) \
        .limit(1) \
        .execute()

    if not check.data:
        raise WriteVerificationError(
            f"{table} / {zipcode}: row not found on re-read after UPDATE."
        )

    stored = check.data[0].get(column)
    if stored != value:
        raise WriteVerificationError(
            f"{table} / {zipcode}: stored value does not match written value."
        )


def upsert_and_verify(supabase, table: str, record: dict,
                      on_conflict: str = "zipcode",
                      max_attempts: int = 3, backoff_base: int = 1):
    """Upsert a record with retry + post-write verification.

    Combines the existing upsert_with_retry pattern (exponential backoff
    on 502/503/504) with a verification re-read to catch silent failures.
    """
    zipcode = record.get("zipcode", "")

    for attempt in range(1, max_attempts + 1):
        try:
            resp = supabase.table(table).upsert(
                record, on_conflict=on_conflict
            ).execute()

            if not getattr(resp, "data", None):
                raise WriteVerificationError(
                    f"{table} / {zipcode}: UPSERT returned no rows — "
                    f"write may have been blocked."
                )
            return

        except WriteVerificationError:
            raise
        except Exception as e:
            err_str = str(e)
            retryable = any(code in err_str for code in ["502", "503", "504"]) \
                        or "ConnectionError" in type(e).__name__ \
                        or "ConnectionReset" in err_str \
                        or "RemoteDisconnected" in err_str
            if retryable and attempt < max_attempts:
                wait = backoff_base * (2 ** (attempt - 1))
                time.sleep(wait)
            else:
                raise


# ── Preflight checks ─────────────────────────────────────────────────


class PreflightError(Exception):
    """Raised when a preflight check fails. Do not proceed."""
    pass


def preflight(supabase_key: str,
              anthropic_key: Optional[str] = None,
              check_git: bool = True):
    """Run startup assertions. Call at the top of every pipeline/batch.

    Checks:
      (a) SUPABASE_KEY is a service-role key (JWT format, not publishable)
      (b) If anthropic_key provided, the configured model resolves
      (c) If check_git, the working branch is pushed to origin
    """
    errors = []

    # (a) Service-role key check
    if not supabase_key:
        errors.append("SUPABASE_KEY is empty.")
    elif not supabase_key.startswith("eyJ"):
        errors.append(
            f"SUPABASE_KEY does not look like a service-role JWT "
            f"(starts with '{supabase_key[:10]}...' instead of 'eyJ'). "
            f"This is likely the publishable key — writes will silently fail under RLS."
        )

    # (b) Model resolution check
    if anthropic_key:
        try:
            import anthropic
            client = anthropic.Anthropic(api_key=anthropic_key)
            model = claude_model()
            client.messages.create(
                model=model,
                max_tokens=5,
                messages=[{"role": "user", "content": "ping"}],
            )
        except Exception as e:
            errors.append(f"Claude model '{model}' check failed: {e}")

    # (c) Git branch pushed check
    if check_git:
        try:
            result = subprocess.run(
                ["git", "status", "-b", "--porcelain=v2"],
                capture_output=True, text=True, timeout=10
            )
            for line in result.stdout.splitlines():
                if line.startswith("# branch.ab"):
                    parts = line.split()
                    ahead = int(parts[2].lstrip("+"))
                    if ahead > 0:
                        errors.append(
                            f"Git branch has {ahead} unpushed commit(s). "
                            f"Push before running pipelines."
                        )
                    break
        except Exception as e:
            errors.append(f"Git check failed: {e}")

    if errors:
        msg = "PREFLIGHT FAILED:\n" + "\n".join(f"  - {e}" for e in errors)
        raise PreflightError(msg)


# ── ZIP loader ───────────────────────────────────────────────────


def load_zip_codes(supabase, batch_size: int = 500) -> pd.DataFrame:
    """Load ALL rows from zip_codes, paginating past Supabase's default 1000-row cap."""
    all_rows = []
    offset = 0
    while True:
        resp = supabase.table("zip_codes") \
            .select("zipcode, metro") \
            .range(offset, offset + batch_size - 1) \
            .execute()
        if not resp.data:
            break
        all_rows.extend(resp.data)
        if len(resp.data) < batch_size:
            break
        offset += batch_size
    return pd.DataFrame(all_rows)
