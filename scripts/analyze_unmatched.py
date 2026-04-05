from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "data" / "raw" / "blog_index_html.csv"
UNMATCHED_DIR = ROOT / "data" / "html_cache" / "unmatched"

pat = re.compile(r'<link\s+rel="canonical"\s+href="([^"]+)"', re.IGNORECASE)

m = pd.read_csv(MANIFEST, dtype=str).fillna("")
missing = m[m["html_exists"] != "1"][["id", "name", "url", "html_file", "source_origin"]].copy()

print("missing_count", len(missing))

files = sorted(UNMATCHED_DIR.glob("*.html"))
print("unmatched_files", len(files))

for p in files:
    text = p.read_text(encoding="utf-8", errors="ignore")
    mm = pat.search(text)
    canonical = mm.group(1).rstrip("/") if mm else ""
    rows = missing[missing["url"].str.rstrip("/") == canonical]

    print("\nFILE", p.name)
    print("canonical", canonical)
    print("rows", len(rows))
    if len(rows):
        print(rows.to_string(index=False))
    else:
        print("NO_ROW")
