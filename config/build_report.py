"""Convert docs/REPORT.md into submission_root/report.pdf (used by `make report`)."""

from __future__ import annotations

import argparse
from pathlib import Path

from markdown_pdf import MarkdownPdf, Section

PROJECT_ROOT = Path(__file__).resolve().parent.parent

CSS = """
body { font-family: sans-serif; font-size: 10pt; line-height: 1.4; }
h1 { font-size: 18pt; }
h2 { font-size: 14pt; margin-top: 14pt; }
h3 { font-size: 12pt; }
code, pre { font-family: monospace; font-size: 8.5pt; }
pre { background: #f4f4f4; padding: 6pt; }
table { border-collapse: collapse; }
th, td { border: 1px solid #999; padding: 3pt 5pt; font-size: 9pt; }
th { background: #e8e8e8; }
"""


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--input", default=str(PROJECT_ROOT / "docs" / "REPORT.md"))
    p.add_argument(
        "--output", default=str(PROJECT_ROOT / "submission_root" / "report.pdf")
    )
    args = p.parse_args()

    src = Path(args.input)
    out = Path(args.output)
    text = src.read_text(encoding="utf-8")

    pdf = MarkdownPdf(toc_level=2)
    pdf.meta["title"] = "LiveRecall Technical Report"
    # root= lets relative image paths in the Markdown resolve from docs/.
    pdf.add_section(Section(text, root=str(src.parent)), user_css=CSS)
    out.parent.mkdir(parents=True, exist_ok=True)
    pdf.save(str(out))
    print(f"Wrote {out.relative_to(PROJECT_ROOT)}")


if __name__ == "__main__":
    main()
