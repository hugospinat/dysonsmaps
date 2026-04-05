from __future__ import annotations

import re
from difflib import SequenceMatcher
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "data" / "raw" / "blog_index_html.csv"
UNMATCHED_DIR = ROOT / "data" / "html_cache" / "unmatched"

CANONICAL_RE = re.compile(r'<link\s+rel="canonical"\s+href="([^"]+)"', re.IGNORECASE)
TITLE_RE = re.compile(r"<title>(.*?)</title>", re.IGNORECASE | re.DOTALL)


def slug_from_url(url: str) -> str:
    if not url:
        return ""
    return urlparse(url).path.rstrip("/").split("/")[-1].lower()


def normalize_text(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip().lower())


def sim(a: str, b: str) -> float:
    if not a and not b:
        return 1.0
    return SequenceMatcher(None, a, b).ratio()


def main() -> None:
    m = pd.read_csv(MANIFEST, dtype=str).fillna("")
    missing = m[m["html_exists"] != "1"].copy()
    missing = missing[["id", "name", "url", "html_file", "source_origin"]]
    missing["url_slug"] = missing["url"].map(slug_from_url)

    print("=== 16 LIGNES MANQUANTES ===")
    print(f"count={len(missing)}")
    print(missing.to_string(index=False))

    unmatched_files = sorted(UNMATCHED_DIR.glob("*.html"))
    print("\n=== 4 FICHIERS HTML UNMATCHED ===")
    for p in unmatched_files:
        print(p.name)

    print("\n=== MATCHING SUGGERE (HEURISTIQUE) ===")
    for p in unmatched_files:
        content = p.read_text(encoding="utf-8", errors="ignore")

        m_c = CANONICAL_RE.search(content)
        canonical = m_c.group(1).rstrip("/") if m_c else ""
        cslug = slug_from_url(canonical)

        m_t = TITLE_RE.search(content)
        title = normalize_text(m_t.group(1)) if m_t else ""

        # filename slug from 0000_slug.html format
        stem = p.stem
        fslug = re.sub(r"^\d+_", "", stem).lower()

        # score missing rows by best of slug/canonical/title
        scored = []
        for _, row in missing.iterrows():
            url_slug = normalize_text(str(row["url_slug"]))
            name = normalize_text(str(row["name"]))
            html_file = normalize_text(str(row["html_file"]).replace(".html", ""))

            score_slug = max(sim(fslug, url_slug), sim(cslug, url_slug), sim(fslug, html_file))
            score_title = sim(title, name)
            score = 0.7 * score_slug + 0.3 * score_title
            scored.append((score, row))

        scored.sort(key=lambda x: x[0], reverse=True)

        print(f"\nFILE: {p.name}")
        print(f"canonical: {canonical}")
        print(f"canonical_slug: {cslug}")
        print(f"filename_slug: {fslug}")

        top = scored[:3]
        for rank, (score, row) in enumerate(top, start=1):
            print(
                f"  {rank}. score={score:.3f} | id={row['id']} | "
                f"name={row['name']} | url={row['url']} | html_file={row['html_file']}"
            )


if __name__ == "__main__":
    main()
