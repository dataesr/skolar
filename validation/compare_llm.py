#!/usr/bin/env python3
"""
compare_runs.py
Compare two LLM inference runs stored under <base_dir>/llm<paragraph_type> and <base_dir>/llm2<paragraph_type>.

Usage:
    python compare_runs.py -p <paragraph_type> --base-dir /path/to/volume --output-dir /path/to/output

Outputs:
    stats.json        — global statistics across both runs
    publications.json   — per-publication paragraph & entities counts
"""

import argparse
import json
from pathlib import Path
import pandas as pd

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

FOLDERS_MAPPING = {"acknowledgements": "acknowledgement", "datasets": "dataset", "software": "software"}
ENTITIES_MAPPING = {"acknowledgements": ["funders", "infrastructures"]}


def load_paragraph(path: Path, paragraph_type: str) -> pd.Series:
    """
    Load a paragraph JSON file as a pandas Series.
    Returns a Series with empty=True and count=0 if the file is empty or invalid.
    """
    data: dict = {"file": path.name, "pub_id": path.stem, "empty": True}
    entities = ENTITIES_MAPPING.get(paragraph_type, [])
    for entity in entities:
        data[f"{entity}_count"] = 0

    try:
        if path.stat().st_size == 0:
            return pd.Series(data)

        s = pd.read_json(path, lines=True, typ="series")

        if not s.empty:
            data["empty"] = False

        for entity in entities:
            entity_list = s.get(entity, [])
            data[f"{entity}_count"] = len(entity_list)

        return pd.Series(data)

    except (ValueError, OSError):
        return pd.Series(data)


def scan_run(run_dir: Path, paragraph_type: str) -> pd.DataFrame:
    """
    Walk a run directory and return a DataFrame with one row per paragraph file.
    Columns: pub_id, file, empty, count
    """
    if not run_dir.exists():
        raise FileNotFoundError(f"Run directory not found: {run_dir}")

    rows = [load_paragraph(p, paragraph_type) for p in sorted(run_dir.rglob("*.jsonl"))]
    if not rows:
        columns = ["pub_id", "file", "empty"]
        columns = columns.extend([f"{entity}_count" for entity in ENTITIES_MAPPING.get(paragraph_type, [])])
        return pd.DataFrame(columns=columns)
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Core comparison
# ---------------------------------------------------------------------------


def compare(base_dir: Path, paragraph_type: str) -> tuple:
    llm_dir = base_dir / "llm" / FOLDERS_MAPPING.get(paragraph_type, paragraph_type)
    llm2_dir = base_dir / "llm2" / FOLDERS_MAPPING.get(paragraph_type, paragraph_type)

    print(f"Scanning {llm_dir} ...")
    llm_df = scan_run(llm_dir, paragraph_type)
    print(f"Scanning {llm2_dir} ...")
    llm2_df = scan_run(llm2_dir, paragraph_type)

    def agg_by_pub(df: pd.DataFrame) -> pd.DataFrame:
        return (
            df.groupby("pub_id")
            .agg(
                total=("file", "count"),
                empty=("empty", "sum"),
                funders=("funders_count", "sum"),  # TODO: should depends of paragraph type
                infras=("infrastructures_count", "sum"),
            )
            .assign(processed=lambda d: d["total"] - d["empty"])
        )

    llm_agg = agg_by_pub(llm_df).add_prefix("llm_")
    llm2_agg = agg_by_pub(llm2_df).add_prefix("llm2_")

    merged = llm_agg.join(llm2_agg, how="outer").fillna(0).astype(int)
    merged.index.name = "publication_id"
    merged = merged.reset_index()

    merged["in_llm"] = merged["publication_id"].isin(llm_df["pub_id"].unique())
    merged["in_llm2"] = merged["publication_id"].isin(llm2_df["pub_id"].unique())
    merged["only_in_one_run"] = merged["in_llm"] != merged["in_llm2"]
    merged["paragraph_diff"] = merged["llm2_processed"] - merged["llm_processed"]

    for entity in ENTITIES_MAPPING.get(paragraph_type, []):
        merged[f"{entity}_diff"] = merged[f"llm2_{entity}"] - merged[f"llm_{entity}"]

    # ------------------------------------------------------------------
    # Meta stats
    # ------------------------------------------------------------------
    both = merged[merged["in_llm"] & merged["in_llm2"]]
    only_llm = merged[merged["in_llm"] & ~merged["in_llm2"]]
    only_llm2 = merged[~merged["in_llm"] & merged["in_llm2"]]

    meta = {
        "publications": {
            "total_unique": len(merged),
            "in_both_runs": len(both),
            "only_in_llm": len(only_llm),
            "only_in_llm2": len(only_llm2),
        },
        "paragraphs": {
            "llm_total": int(merged["llm_total"].sum()),
            "llm2_total": int(merged["llm2_total"].sum()),
            "llm_empty": int(merged["llm_empty"].sum()),
            "llm2_empty": int(merged["llm2_empty"].sum()),
            "llm_processed": int(merged["llm_processed"].sum()),
            "llm2_processed": int(merged["llm2_processed"].sum()),
        },
    }

    per_pub = merged.where(merged.notna()).to_dict(orient="records")
    return meta, per_pub


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(description="Compare two LLM inference runs.")
    parser.add_argument("--paragraph", "-p", type=str, required=True, help="Paragraph type.")
    parser.add_argument("--base-dir", type=Path, default=Path("/data"), help="Data directory.")
    parser.add_argument("--output-dir", type=Path, default=Path("/data"), help="Output directory.")
    args = parser.parse_args()

    meta, per_pub = compare(args.base_dir, args.paragraph)

    meta_path: Path = args.output_dir / "compare" / FOLDERS_MAPPING.get(args.paragraph, args.paragraph) / "stats.json"
    pub_path: Path = args.output_dir / "compare" / FOLDERS_MAPPING.get(args.paragraph, args.paragraph) / "publications.json"

    # args.output_dir.mkdir(parents=True, exist_ok=True)
    meta_path.parent.mkdir(parents=True, exist_ok=True)
    pub_path.parent.mkdir(parents=True, exist_ok=True)

    with open(meta_path, "w") as f:
        json.dump(meta, f, indent=2)
    print(f"Written: {meta_path}")

    with open(pub_path, "w") as f:
        json.dump(per_pub, f, indent=2, ensure_ascii=False)
    print(f"Written: {pub_path}")

    # Quick summary to stdout
    print("\n=== Summary ===")
    print(json.dumps(meta, indent=2))


if __name__ == "__main__":
    main()
