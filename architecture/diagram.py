"""
Architecture Diagram Generator
================================
Produces a visual architecture diagram of the Golden Order Data Pipeline
as an SVG file (no external dependencies — pure Python stdlib).

Run:  python -m architecture.diagram
Output: architecture/pipeline_architecture.svg
        architecture/pipeline_architecture.html  (browser-viewable)
"""

from pathlib import Path

# ── SVG helpers ────────────────────────────────────────────────────────────────

def _rect(x, y, w, h, fill, stroke="#555", rx=8, ry=8, stroke_width=1.5,
          stroke_dash=""):
    dash = f'stroke-dasharray="{stroke_dash}"' if stroke_dash else ""
    return (
        f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{rx}" ry="{ry}" '
        f'fill="{fill}" stroke="{stroke}" stroke-width="{stroke_width}" {dash}/>'
    )


def _text(x, y, content, size=12, fill="white", weight="normal", anchor="middle"):
    return (
        f'<text x="{x}" y="{y}" text-anchor="{anchor}" '
        f'font-size="{size}" fill="{fill}" font-weight="{weight}" '
        f'font-family="monospace">{content}</text>'
    )


def _arrow(x1, y1, x2, y2, color="#888", label=""):
    """Draw a line with an arrowhead at (x2,y2)."""
    # Direction vector
    dx, dy = x2 - x1, y2 - y1
    length = (dx*dx + dy*dy) ** 0.5
    if length == 0:
        return ""
    ux, uy = dx / length, dy / length
    # Arrowhead (filled triangle)
    arrow_size = 8
    ax = x2 - ux * arrow_size
    ay = y2 - uy * arrow_size
    # Perpendicular
    px, py = -uy * 4, ux * 4
    pts = f"{x2},{y2} {ax+px},{ay+py} {ax-px},{ay-py}"
    lbl = ""
    if label:
        mx, my = (x1 + x2) / 2, (y1 + y2) / 2
        lbl = _text(mx + 4, my - 4, label, size=10, fill="#aaa")
    return (
        f'<line x1="{x1}" y1="{y1}" x2="{ax}" y2="{ay}" '
        f'stroke="{color}" stroke-width="1.5"/>'
        f'<polygon points="{pts}" fill="{color}"/>'
        + lbl
    )


def _box(x, y, w, h, label, sublabel="", fill="#2d2d2d", text_color="white",
         stroke="#666", label_size=12, sublabel_size=10):
    """Draw a box with label (and optional sub-label)."""
    elements = [_rect(x, y, w, h, fill=fill, stroke=stroke)]
    label_y = y + h // 2 + (label_size // 3) - (5 if sublabel else 0)
    elements.append(_text(x + w // 2, label_y, label,
                           size=label_size, fill=text_color, weight="bold"))
    if sublabel:
        elements.append(_text(x + w // 2, label_y + 18, sublabel,
                               size=sublabel_size, fill="#bbb"))
    return "\n".join(elements)


# ── Colour palette ────────────────────────────────────────────────────────────
C_BG        = "#1a1a2e"
C_SOURCE    = "#16213e"
C_INGEST    = "#0f3460"
C_LAKE      = "#1a472a"
C_TRANSFORM = "#4a235a"
C_KAFKA     = "#922b21"
C_SPARK     = "#a04000"
C_WARE      = "#1b4f72"
C_BI        = "#145a32"
C_GOLD      = "#c8a96e"
C_RED_STAR  = "#6c1f1f"


def generate_svg() -> str:
    W, H = 1100, 780
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" '
        f'viewBox="0 0 {W} {H}">',
        f'<rect width="{W}" height="{H}" fill="{C_BG}"/>',
    ]

    # ── Title ─────────────────────────────────────────────────────────────────
    parts.append(_text(W // 2, 38, "THE GOLDEN ORDER DATA PIPELINE",
                        size=22, fill=C_GOLD, weight="bold"))
    parts.append(_text(W // 2, 62, "Elden Ring Combat &amp; Build Analytics — Architecture",
                        size=13, fill="#888"))

    # ── Row 1: Data Sources ───────────────────────────────────────────────────
    parts.append(_text(W // 2, 95, "DATA SOURCES", size=11, fill="#666"))
    parts.append(_box(30,  105, 280, 60, "Elden Ring Fan API",
                       "REST / JSON / paginated", C_SOURCE))
    parts.append(_box(400, 105, 280, 60, "Kaggle Dataset",
                       "CSV / eldenringScrap/", C_SOURCE))
    parts.append(_box(775, 105, 280, 60, "Telemetry Generator",
                       "Synthetic / 500 events/s", C_SOURCE))

    # ── Row 2: Ingestion ──────────────────────────────────────────────────────
    parts.append(_text(W // 2, 202, "INGESTION  (Phase 1)", size=11, fill="#666"))
    parts.append(_box(30,  212, 280, 60, "api_extractor.py",
                       "retry / pagination / idempotent", C_INGEST))
    parts.append(_box(400, 212, 280, 60, "kaggle_loader.py",
                       "CSV → Parquet / data/staged/", C_INGEST))
    parts.append(_box(775, 212, 280, 60, "kafka_producer.py",
                       "confluent-kafka / LZ4 / async", C_KAFKA))

    # Row 1→2 arrows
    parts.append(_arrow(170, 165, 170, 212))
    parts.append(_arrow(540, 165, 540, 212))
    parts.append(_arrow(915, 165, 915, 212))

    # ── Row 3: Storage ────────────────────────────────────────────────────────
    parts.append(_text(W // 2, 310, "STORAGE  (Data Lake + Message Broker)", size=11, fill="#666"))
    parts.append(_box(30,  320, 280, 60, "Data Lake",
                       "data/raw/{endpoint}/{date}/*.json", C_LAKE))
    parts.append(_box(400, 320, 280, 60, "Staging Area",
                       "data/staged/*.parquet (Snappy)", C_LAKE))
    parts.append(_box(775, 320, 280, 60, "Kafka Topic",
                       "player_encounters (6 partitions)", C_KAFKA, stroke=C_GOLD))

    parts.append(_arrow(170, 272, 170, 320))
    parts.append(_arrow(540, 272, 540, 320))
    parts.append(_arrow(915, 272, 915, 320))

    # ── Row 4: Transformation ─────────────────────────────────────────────────
    parts.append(_text(W // 2, 418, "TRANSFORMATION  (Phase 2 + 3)", size=11, fill="#666"))
    parts.append(_box(30,  428, 280, 60, "json_flattener.py",
                       "Parse nested dicts → typed cols", C_TRANSFORM))
    parts.append(_box(390, 423, 300, 70, "★  scaling_engine.py  ★",
                       "Soft-cap AR UDF (piecewise lerp)", C_RED_STAR,
                       text_color="#f5c518", stroke=C_GOLD, label_size=13))
    parts.append(_box(775, 428, 280, 60, "spark_consumer.py",
                       "Structured Streaming / foreachBatch", C_SPARK))

    parts.append(_arrow(170, 380, 170, 428))
    parts.append(_arrow(540, 380, 540, 428))
    parts.append(_arrow(915, 380, 915, 428))
    # json → scaling engine, scaling engine → spark
    parts.append(_arrow(310, 458, 390, 458))
    parts.append(_arrow(690, 458, 775, 458, C_GOLD, "UDF"))

    # ── Row 5: Warehouse (Star Schema) ────────────────────────────────────────
    # Airflow bracket outline
    parts.append(_rect(15, 100, 1070, 450, fill="none", stroke=C_GOLD,
                        stroke_dash="6,4", rx=12, ry=12, stroke_width=1.5))
    parts.append(_text(880, 558, "Airflow DAG  @daily  |  golden_order_pipeline",
                        size=11, fill=C_GOLD, anchor="middle"))

    parts.append(_text(W // 2, 580, "DATA WAREHOUSE  (golden_order schema  —  PostgreSQL / BigQuery)",
                        size=11, fill="#5d8aa8"))
    parts.append(_rect(15, 588, 1070, 80, fill="#0d1b2a", stroke="#1b4f72",
                        rx=8, ry=8, stroke_width=1))

    parts.append(_box(30,  595, 280, 65, "dim_weapons",
                       "60K rows | AR pre-computed | +25 max", C_WARE))
    parts.append(_box(390, 595, 300, 65, "dim_bosses",
                       "154 rows | HP + drops + DLC flag", C_WARE))
    parts.append(_box(775, 595, 280, 65, "fact_encounters",
                       "Streaming sink | event_id UNIQUE", C_WARE, stroke=C_GOLD))

    parts.append(_arrow(540, 488, 540, 595))
    parts.append(_arrow(170, 488, 170, 595))
    parts.append(_arrow(915, 488, 915, 595))

    # ── Row 6: Analytics ──────────────────────────────────────────────────────
    parts.append(_text(W // 2, 698, "ANALYTICS  (Phase 4)", size=11, fill="#666"))
    parts.append(_box(100, 706, 350, 60, "Analytics Views (SQL)",
                       "v_boss_lethality / v_weapon_win_rates", C_BI))
    parts.append(_box(620, 706, 350, 60, "Metabase Dashboard",
                       "Most Lethal Bosses | Best Builds", C_BI, stroke=C_GOLD))

    parts.append(_arrow(275, 660, 275, 706))
    parts.append(_arrow(800, 660, 800, 706))
    parts.append(_arrow(450, 736, 620, 736))

    # ── Legend ────────────────────────────────────────────────────────────────
    legend_items = [
        (C_SOURCE, "Data Source"),
        (C_INGEST, "Ingestion (Phase 1)"),
        (C_LAKE,   "Data Lake / Staging"),
        (C_RED_STAR, "★ Scaling Engine"),
        (C_SPARK,  "Stream Processing"),
        (C_WARE,   "Data Warehouse"),
        (C_BI,     "Analytics / BI"),
        (C_KAFKA,  "Kafka Broker"),
    ]
    lx, ly = 15, 718
    for i, (color, label) in enumerate(legend_items):
        col = i % 4
        row = i // 4
        bx = lx + col * 130
        by = ly + row * 22
        parts.append(_rect(bx, by, 14, 14, fill=color, stroke="#666", rx=2, ry=2, stroke_width=1))
        parts.append(_text(bx + 20, by + 11, label, size=10, fill="#aaa", anchor="start"))

    parts.append("</svg>")
    return "\n".join(parts)


def save_svg(out_path: Path) -> None:
    svg = generate_svg()
    out_path.write_text(svg, encoding="utf-8")
    print(f"SVG diagram saved: {out_path}")

    # Also write a simple HTML wrapper so it can be opened directly in a browser
    html_path = out_path.with_suffix(".html")
    html = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8">
<title>Golden Order Data Pipeline — Architecture</title>
<style>body{{margin:0;background:#1a1a2e;display:flex;justify-content:center;padding:20px}}</style>
</head><body>
{svg}
</body></html>"""
    html_path.write_text(html, encoding="utf-8")
    print(f"HTML diagram saved: {html_path}")


if __name__ == "__main__":
    out = Path(__file__).parent / "pipeline_architecture.svg"
    save_svg(out)
