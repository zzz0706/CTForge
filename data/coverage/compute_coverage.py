
from __future__ import annotations
from pathlib import Path
from typing import List, Tuple, Optional, Literal
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
    Compute average line & branch coverage from a JSON object with structure:
      { "details": [ { "line_coverage": float, "branch_coverage": float, ... }, ... ] }
    Modes:
      - "include": include zeros in the average (missing/null skipped).
      - "exclude": exclude zeros (filter out exact 0.0 values).
      - "both": return both results.
    Returns a dict with keys depending on mode, e.g.:
      {
        "include": {"line_avg": 0.123, "branch_avg": 0.456, "counts": {"line": 80, "branch": 80}},
        "exclude": {"line_avg": 0.234, "branch_avg": 0.567, "counts_nonzero": {"line": 60, "branch": 58}}
      }
    """
    details = data.get("details", [])
    line_vals = _extract_values(details, "line_coverage")
    branch_vals = _extract_values(details, "branch_coverage")

    def include_zero() -> dict:
        return {
            "line_avg": _mean(line_vals),
            "branch_avg": _mean(branch_vals),
            "counts": {"line": len(line_vals), "branch": len(branch_vals)}
        }

    def exclude_zero() -> dict:
        nz_line = [v for v in line_vals if v != 0.0]
        nz_branch = [v for v in branch_vals if v != 0.0]
        return {
            "line_avg": _mean(nz_line),
            "branch_avg": _mean(nz_branch),
            "counts_nonzero": {"line": len(nz_line), "branch": len(nz_branch)},
            "note": "Zeros excluded; if all values are zero/missing, averages are 0.0."
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
    parser = argparse.ArgumentParser(description="Compute line & branch coverage averages from JSON.")
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
        la = block["line_avg"]
        ba = block["branch_avg"]
        print(f"Line Avg : {_format_pct(la)}")
        print(f"Branch Avg: {_format_pct(ba)}")
        extra_counts = block.get("counts") or block.get("counts_nonzero") or {}
        print(f"Counts    : {extra_counts}")
        note = block.get("note")
        if note:
            print(f"Note      : {note}")

    if "include" in res:
        print_block("INCLUDE ZEROS", res["include"])
    if "exclude" in res:
        print_block("EXCLUDE ZEROS", res["exclude"])

if __name__ == "__main__":
    main()
