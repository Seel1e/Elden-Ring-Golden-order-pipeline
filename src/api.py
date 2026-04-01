"""
Golden Order — FastAPI REST Service
=====================================
Run with:   uvicorn src.api:app --reload --port 8000

Endpoints:
  GET  /health
  GET  /api/classes
  GET  /api/weapons
  GET  /api/bosses
  POST /api/ar                   — calculate Attack Rating
  GET  /api/bosses/gauntlet      — rank all bosses for a build
  GET  /api/meta                 — meta report
  GET  /api/builds               — list saved builds
  GET  /api/builds/{name}        — get one saved build
  POST /api/builds               — save a build
  DELETE /api/builds/{name}      — delete a build
"""

from __future__ import annotations

from typing import Optional
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import pandas as pd
from sqlalchemy import create_engine, text

from src.config import WarehouseConfig
from src.transformation.scaling_engine import (
    WeaponScaling, PlayerStats, calculate_attack_rating,
)

# ── App ───────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="Golden Order API",
    description="Elden Ring Combat Analytics — build calculator + live pipeline data",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── DB ────────────────────────────────────────────────────────────────────────
_engine = None

def get_engine():
    global _engine
    if _engine is None:
        _engine = create_engine(WarehouseConfig.dsn())
    return _engine


def qry(sql: str, params: dict | None = None) -> list[dict]:
    df = pd.read_sql(text(sql), get_engine(), params=params or {})
    return df.to_dict(orient="records")


# ── Pydantic models ───────────────────────────────────────────────────────────

class ARRequest(BaseModel):
    weapon_name:   str
    upgrade_level: int
    strength:      int = 10
    dexterity:     int = 10
    intelligence:  int = 10
    faith:         int = 10
    arcane:        int = 10
    two_handing:   bool = False


class BuildRequest(BaseModel):
    build_name:      str
    class_name:      Optional[str] = None
    character_level: Optional[int] = None
    strength:        int = 10
    dexterity:       int = 10
    intelligence:    int = 10
    faith:           int = 10
    arcane:          int = 10
    weapon_name:     str
    upgrade_level:   int
    two_handing:     bool = False


# ── Helpers ───────────────────────────────────────────────────────────────────

def _fetch_weapon(weapon_name: str, upgrade_level: int) -> pd.Series:
    df = pd.read_sql(text("""
        SELECT base_physical, base_magic, base_fire, base_lightning, base_holy,
               scale_str, scale_dex, scale_int, scale_fai, scale_arc
        FROM golden_order.dim_weapons
        WHERE weapon_name = :w AND upgrade_level = :u LIMIT 1
    """), get_engine(), params={"w": weapon_name, "u": upgrade_level})
    if df.empty:
        raise HTTPException(404, f"Weapon '{weapon_name}' at upgrade +{upgrade_level} not found")
    return df.iloc[0]


def _compute_ar(row: pd.Series, req: ARRequest) -> dict:
    w = WeaponScaling(
        base_physical  = float(row.get("base_physical")  or 0),
        base_magic     = float(row.get("base_magic")     or 0),
        base_fire      = float(row.get("base_fire")      or 0),
        base_lightning = float(row.get("base_lightning") or 0),
        base_holy      = float(row.get("base_holy")      or 0),
        scaling={
            "Str": str(row.get("scale_str") or "-"),
            "Dex": str(row.get("scale_dex") or "-"),
            "Int": str(row.get("scale_int") or "-"),
            "Fai": str(row.get("scale_fai") or "-"),
            "Arc": str(row.get("scale_arc") or "-"),
        },
    )
    p = PlayerStats(
        strength=req.strength, dexterity=req.dexterity,
        intelligence=req.intelligence, faith=req.faith,
        arcane=req.arcane, two_handing=req.two_handing,
    )
    return calculate_attack_rating(w, p)


# ── Routes ────────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok", "timestamp": datetime.now(timezone.utc).isoformat()}


@app.get("/api/classes")
def get_classes():
    """All 10 Elden Ring starting classes with base stats."""
    return qry("SELECT * FROM golden_order.dim_classes ORDER BY base_level")


@app.get("/api/weapons")
def get_weapons(category: Optional[str] = None):
    """List all weapons, optionally filtered by category."""
    if category:
        return qry("""
            SELECT DISTINCT weapon_name, category
            FROM golden_order.dim_weapons
            WHERE LOWER(category) = LOWER(:c)
            ORDER BY weapon_name
        """, {"c": category})
    return qry("SELECT DISTINCT weapon_name, category FROM golden_order.dim_weapons ORDER BY weapon_name")


@app.get("/api/weapons/{weapon_name}/upgrades")
def get_weapon_upgrades(weapon_name: str):
    """Get available upgrade levels for a specific weapon."""
    rows = qry("""
        SELECT upgrade_level, base_physical, base_magic, base_fire,
               base_lightning, base_holy, scale_str, scale_dex,
               scale_int, scale_fai, scale_arc, category, weight
        FROM golden_order.dim_weapons
        WHERE weapon_name = :w ORDER BY upgrade_level
    """, {"w": weapon_name})
    if not rows:
        raise HTTPException(404, f"Weapon '{weapon_name}' not found")
    return rows


@app.get("/api/bosses")
def get_bosses(dlc_only: bool = False):
    """List all bosses with HP and DLC flag."""
    sql = "SELECT * FROM golden_order.dim_bosses"
    if dlc_only:
        sql += " WHERE is_dlc = true"
    sql += " ORDER BY boss_name"
    return qry(sql)


@app.get("/api/bosses/{boss_name}/stats")
def get_boss_stats(boss_name: str):
    """Community stats for one boss from the pipeline."""
    rows = qry("""
        SELECT
            boss_name,
            COUNT(*) AS encounters,
            ROUND(100.0 * SUM(CASE WHEN NOT is_victory THEN 1 ELSE 0 END)::numeric / COUNT(*), 2) AS death_rate_pct,
            ROUND(AVG(calculated_ar), 1) AS avg_ar,
            ROUND(AVG(player_level), 1) AS avg_player_level
        FROM golden_order.fact_encounters
        WHERE boss_name = :b
        GROUP BY boss_name
    """, {"b": boss_name})
    if not rows:
        raise HTTPException(404, f"No encounters found for '{boss_name}'")
    return rows[0]


@app.post("/api/ar")
def calculate_ar(req: ARRequest):
    """
    Calculate Attack Rating for a weapon + player build.

    Example body:
        {"weapon_name": "Rivers of Blood", "upgrade_level": 25,
         "dexterity": 40, "arcane": 60}
    """
    row = _fetch_weapon(req.weapon_name, req.upgrade_level)
    ar  = _compute_ar(row, req)

    # Soft cap advice
    advice = []
    stat_fields = [
        ("Strength",     "strength",     req.strength),
        ("Dexterity",    "dexterity",    req.dexterity),
        ("Intelligence", "intelligence", req.intelligence),
        ("Faith",        "faith",        req.faith),
        ("Arcane",       "arcane",       req.arcane),
    ]
    for label, field, val in stat_fields:
        bumped = ARRequest(**{**req.model_dump(), field: min(val + 5, 99)})
        gain = _compute_ar(row, bumped)["total"] - ar["total"]
        advice.append({"stat": label, "current": val, "ar_gain_plus5": round(gain, 2)})

    advice.sort(key=lambda x: x["ar_gain_plus5"], reverse=True)
    return {"ar": ar, "soft_cap_advice": advice}


@app.get("/api/bosses/gauntlet")
def boss_gauntlet(
    weapon:    str = Query(..., description="Weapon name"),
    upgrade:   int = Query(25,  description="Upgrade level"),
    strength:  int = Query(10),
    dexterity: int = Query(10),
    intelligence: int = Query(10),
    faith:     int = Query(10),
    arcane:    int = Query(10),
    two_handing: bool = Query(False),
):
    """
    Rank all bosses from easiest to hardest for a given build.

    Example:
        /api/bosses/gauntlet?weapon=Rivers+of+Blood&upgrade=25&dexterity=40&arcane=60
    """
    req = ARRequest(weapon_name=weapon, upgrade_level=upgrade,
                    strength=strength, dexterity=dexterity,
                    intelligence=intelligence, faith=faith,
                    arcane=arcane, two_handing=two_handing)
    row = _fetch_weapon(weapon, upgrade)
    ar  = _compute_ar(row, req)["total"]

    bosses = pd.read_sql(text("""
        SELECT b.boss_name, b.hp, b.is_dlc,
               COALESCE(e.death_rate, 50.0) AS death_rate,
               COALESCE(e.encounters, 0) AS encounters
        FROM golden_order.dim_bosses b
        LEFT JOIN (
            SELECT boss_name,
                   COUNT(*) AS encounters,
                   ROUND(100.0 * SUM(CASE WHEN NOT is_victory THEN 1 ELSE 0 END)::numeric / COUNT(*), 2)
                       AS death_rate
            FROM golden_order.fact_encounters
            GROUP BY boss_name HAVING COUNT(*) >= 20
        ) e ON b.boss_name = e.boss_name
        ORDER BY b.boss_name
    """), get_engine())

    bosses["hits_to_kill"] = (bosses["hp"] / ar).round(1)
    bosses["difficulty_score"] = (
        bosses["death_rate"] * 0.7 + bosses["hits_to_kill"].clip(upper=100) * 0.3
    ).round(2)
    bosses = bosses.sort_values("difficulty_score").reset_index(drop=True)
    bosses["rank"] = bosses.index + 1

    return {
        "your_ar": round(ar, 1),
        "weapon": weapon,
        "upgrade": upgrade,
        "bosses": bosses.to_dict(orient="records"),
    }


@app.get("/api/meta")
def get_meta_report():
    """Current meta: top builds, hardest boss, most used weapon, archetype tier list."""
    top_builds = qry("""
        SELECT weapon_used, upgrade_level, category, archetype,
               COUNT(*) AS uses,
               ROUND(100.0 * SUM(CASE WHEN is_victory THEN 1 ELSE 0 END)::numeric / COUNT(*), 1) AS win_rate,
               ROUND(AVG(calculated_ar), 1) AS avg_ar
        FROM golden_order.fact_encounters
        WHERE weapon_used IS NOT NULL
        GROUP BY weapon_used, upgrade_level, category, archetype
        HAVING COUNT(*) >= 30
        ORDER BY win_rate DESC LIMIT 10
    """)

    hardest_boss = qry("""
        SELECT boss_name,
               COUNT(*) AS encounters,
               ROUND(100.0 * SUM(CASE WHEN NOT is_victory THEN 1 ELSE 0 END)::numeric / COUNT(*), 1) AS death_rate
        FROM golden_order.fact_encounters
        GROUP BY boss_name HAVING COUNT(*) >= 50
        ORDER BY death_rate DESC LIMIT 1
    """)

    most_used = qry("""
        SELECT weapon_used, COUNT(*) AS uses
        FROM golden_order.fact_encounters
        WHERE weapon_used IS NOT NULL
        GROUP BY weapon_used ORDER BY uses DESC LIMIT 5
    """)

    archetype_tier = qry("""
        SELECT archetype,
               ROUND(100.0 * SUM(CASE WHEN is_victory THEN 1 ELSE 0 END)::numeric / COUNT(*), 1) AS win_rate,
               ROUND(AVG(calculated_ar), 1) AS avg_ar,
               COUNT(*) AS encounters
        FROM golden_order.fact_encounters
        GROUP BY archetype ORDER BY win_rate DESC
    """)

    return {
        "generated_at":   datetime.now(timezone.utc).isoformat(),
        "top_10_builds":  top_builds,
        "hardest_boss":   hardest_boss[0] if hardest_boss else None,
        "most_used_weapons": most_used,
        "archetype_tier_list": archetype_tier,
    }


@app.get("/api/builds")
def list_builds():
    """List all saved builds ordered by AR."""
    return qry("""
        SELECT build_name, class_name, character_level,
               strength, dexterity, intelligence, faith, arcane,
               weapon_name, upgrade_level, two_handing,
               ROUND(calculated_ar::numeric, 1) AS calculated_ar,
               saved_at
        FROM golden_order.fact_builds ORDER BY calculated_ar DESC
    """)


@app.get("/api/builds/{build_name}")
def get_build(build_name: str):
    """Get a specific saved build by name."""
    rows = qry("SELECT * FROM golden_order.fact_builds WHERE build_name = :n", {"n": build_name})
    if not rows:
        raise HTTPException(404, f"Build '{build_name}' not found")
    return rows[0]


@app.post("/api/builds", status_code=201)
def save_build(req: BuildRequest):
    """Save or update a build by name."""
    row = _fetch_weapon(req.weapon_name, req.upgrade_level)
    ar_req = ARRequest(
        weapon_name=req.weapon_name, upgrade_level=req.upgrade_level,
        strength=req.strength, dexterity=req.dexterity,
        intelligence=req.intelligence, faith=req.faith,
        arcane=req.arcane, two_handing=req.two_handing,
    )
    ar = _compute_ar(row, ar_req)["total"]

    with get_engine().begin() as conn:
        conn.execute(text("""
            INSERT INTO golden_order.fact_builds
                (build_name, class_name, character_level, strength, dexterity,
                 intelligence, faith, arcane, weapon_name, upgrade_level, two_handing, calculated_ar)
            VALUES (:bn,:cn,:cl,:s,:d,:i,:f,:a,:w,:u,:t,:ar)
            ON CONFLICT (build_name) DO UPDATE SET
                class_name=EXCLUDED.class_name, character_level=EXCLUDED.character_level,
                strength=EXCLUDED.strength, dexterity=EXCLUDED.dexterity,
                intelligence=EXCLUDED.intelligence, faith=EXCLUDED.faith,
                arcane=EXCLUDED.arcane, weapon_name=EXCLUDED.weapon_name,
                upgrade_level=EXCLUDED.upgrade_level, two_handing=EXCLUDED.two_handing,
                calculated_ar=EXCLUDED.calculated_ar, saved_at=NOW()
        """), dict(bn=req.build_name, cn=req.class_name, cl=req.character_level,
                   s=req.strength, d=req.dexterity, i=req.intelligence,
                   f=req.faith, a=req.arcane, w=req.weapon_name,
                   u=req.upgrade_level, t=req.two_handing, ar=ar))

    return {"message": f"Build '{req.build_name}' saved", "calculated_ar": round(ar, 1)}


@app.delete("/api/builds/{build_name}")
def delete_build(build_name: str):
    """Delete a saved build."""
    with get_engine().begin() as conn:
        result = conn.execute(
            text("DELETE FROM golden_order.fact_builds WHERE build_name = :n"),
            {"n": build_name}
        )
    if result.rowcount == 0:
        raise HTTPException(404, f"Build '{build_name}' not found")
    return {"message": f"Build '{build_name}' deleted"}
