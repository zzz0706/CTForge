from __future__ import annotations
from pathlib import Path
from typing import List, Literal
import json
import argparse

Mode = Literal["include", "exclude", "both"]

def _extract_values(details: List[dict], key: str) -> List[float]:
    vals: List[float] = []
    for d in details:
        v = d.get(key, None)
        if v is None:
            continue
        try:
            v = float(v)
        except Exception:
            continue
        vals.append(v)
    return vals

def _mean(values: List[float]) -> float:
    if not values:
        return 0.0
    return sum(values) / len(values)

def compute_averages(data: dict, mode: Mode = "both") -> dict:
    """
    Compute average statement and branch coverage from a JSON object with structure:
      { "details": [ { "sc": float, "bc": float, ... }, ... ] }
    Modes:
      - "include": include zeros in the average (missing/null skipped).
      - "exclude": exclude zeros (filter out exact 0.0 values).
      - "both": return both results.
    Returns a dict with keys depending on mode, e.g.:
      {
        "include": {"sc_avg": 0.123, "bc_avg": 0.456, "counts": {"sc": 80, "bc": 80}},
        "exclude": {"sc_avg": 0.234, "bc_avg": 0.567, "counts_nonzero": {"sc": 60, "bc": 58}}
      }
    """
    details = data.get("details", [])
    sc_vals = _extract_values(details, "sc")
    bc_vals = _extract_values(details, "bc")

    def include_zero() -> dict:
        return {
            "sc_avg": _mean(sc_vals),
            "bc_avg": _mean(bc_vals),
            "counts": {"sc": len(sc_vals), "bc": len(bc_vals)}
        }

    def exclude_zero() -> dict:
        nz_sc = [v for v in sc_vals if v != 0.0]
        nz_bc = [v for v in bc_vals if v != 0.0]
        return {
            "sc_avg": _mean(nz_sc),
            "bc_avg": _mean(nz_bc),
            "counts_nonzero": {"sc": len(nz_sc), "bc": len(nz_bc)},
            "note": "Zeros excluded; if all values are zero/missing, averages are 0.0.",
        }

    if mode == "include":
        return {"include": include_zero()}
    elif mode == "exclude":
        return {"exclude": exclude_zero()}
    else:
        return {"include": include_zero(), "exclude": exclude_zero()}

def _format_pct(x: float) -> str:
    return f"{x:.4f} ({x*100:.2f}%)"

def main():
    parser = argparse.ArgumentParser(description="Compute statement & branch coverage averages from JSON.")
    parser.add_argument("json_path", type=str, help="Path to JSON file")
    parser.add_argument("--mode", choices=["include", "exclude", "both"], default="both",
                        help="Include zeros, exclude zeros, or both (default)")
    args = parser.parse_args()

    p = Path(args.json_path)
    if not p.exists():
        raise SystemExit(f"File not found: {p}")

    with p.open("r", encoding="utf-8") as f:
        data = json.load(f)

    res = compute_averages(data, mode=args.mode)

    # Pretty print
    def print_block(title: str, block: dict):
        print(f"\n[{title}]")
        sa = block["sc_avg"]
        ba = block["bc_avg"]
        print(f"SC Avg : {_format_pct(sa)}")
        print(f"BC Avg : {_format_pct(ba)}")
        extra_counts = block.get("counts") or block.get("counts_nonzero") or {}
        print(f"Counts  : {extra_counts}")
        note = block.get("note")
        if note:
            print(f"Note    : {note}")

    if "include" in res:
        print_block("INCLUDE ZEROS", res["include"])
    if "exclude" in res:
        print_block("EXCLUDE ZEROS", res["exclude"])

if __name__ == "__main__":
    main()
