from __future__ import annotations

from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "data" / "raw" / "blog_index_html.csv"
UNMATCHED_DIR = ROOT / "data" / "html_cache" / "unmatched"

# Selected from canonical+slug analysis with highest-confidence unique matches.
MATCHES = {
    "0456_rosewood-street-sewers.html": "735",
    "1482_extreme-dungeon-makeover-dyson-edition.html": "1959",
    "1976_dungeon23-dysons-delve-ii-level-2-complete.html": "495",
    "2057_dungeonmorphs-lairs-set-3.html": "485",
}


def main() -> None:
    if not MANIFEST.exists():
        raise RuntimeError(f"Missing manifest: {MANIFEST}")

    df = pd.read_csv(MANIFEST, dtype=str).fillna("")

    moved = 0
    skipped = 0
    updates = 0

    for src_name, target_id in MATCHES.items():
        src_path = UNMATCHED_DIR / src_name
        if not src_path.exists():
            print(f"SKIP missing source file: {src_name}")
            skipped += 1
            continue

        row = df[df["id"].astype(str) == str(target_id)]
        if row.empty:
            print(f"SKIP missing target row id: {target_id}")
            skipped += 1
            continue

        idx = row.index[0]
        target_file = str(df.at[idx, "html_file"]).strip()
        if not target_file:
            print(f"SKIP empty html_file for id: {target_id}")
            skipped += 1
            continue

        target_path = ROOT / "data" / "html_cache" / target_file
        if target_path.exists():
            print(f"SKIP destination exists: {target_file}")
            skipped += 1
            continue

        target_path.parent.mkdir(parents=True, exist_ok=True)
        src_path.rename(target_path)
        moved += 1

        df.at[idx, "html_exists"] = "1"
        df.at[idx, "html_path"] = str(target_path.resolve())
        df.at[idx, "source_origin"] = "manual_unmatched_match"
        updates += 1

        print(f"MATCHED {src_name} -> id={target_id} file={target_file}")

    df.to_csv(MANIFEST, index=False, encoding="utf-8-sig")

    print("---")
    print(f"moved={moved}")
    print(f"updated_rows={updates}")
    print(f"skipped={skipped}")


if __name__ == "__main__":
    main()
