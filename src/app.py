"""
Golden Order — Streamlit Web App
=================================
Run with:   streamlit run src/app.py
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine, text

from src.config import WarehouseConfig
from src.transformation.scaling_engine import (
    WeaponScaling, PlayerStats, calculate_attack_rating,
)

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Golden Order Analytics",
    page_icon="⚔️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── DB connection (cached) ────────────────────────────────────────────────────
@st.cache_resource
def get_engine():
    return create_engine(WarehouseConfig.dsn())


@st.cache_data(ttl=60)
def load_weapons():
    return pd.read_sql(
        text("SELECT DISTINCT weapon_name FROM golden_order.dim_weapons ORDER BY weapon_name"),
        get_engine()
    )["weapon_name"].tolist()


@st.cache_data(ttl=60)
def load_bosses():
    return pd.read_sql(
        text("SELECT boss_name, hp, is_dlc FROM golden_order.dim_bosses ORDER BY boss_name"),
        get_engine()
    )


@st.cache_data(ttl=60)
def load_classes():
    return pd.read_sql(
        text("SELECT * FROM golden_order.dim_classes ORDER BY base_level"),
        get_engine()
    )


@st.cache_data(ttl=30)
def load_boss_stats():
    return pd.read_sql(text("""
        SELECT
            boss_name,
            COUNT(*) AS encounters,
            ROUND(100.0 * SUM(CASE WHEN NOT is_victory THEN 1 ELSE 0 END)::numeric / COUNT(*), 1) AS death_rate,
            ROUND(AVG(calculated_ar), 1) AS avg_ar,
            ROUND(AVG(player_level), 1) AS avg_level,
            MAX(is_dlc::int)::bool AS is_dlc
        FROM golden_order.fact_encounters
        GROUP BY boss_name
        HAVING COUNT(*) >= 20
        ORDER BY death_rate DESC
    """), get_engine())


@st.cache_data(ttl=30)
def load_weapon_stats():
    return pd.read_sql(text("""
        SELECT weapon_used, upgrade_level, category, archetype,
               COUNT(*) AS uses,
               ROUND(100.0 * SUM(CASE WHEN is_victory THEN 1 ELSE 0 END)::numeric / COUNT(*), 1) AS win_rate,
               ROUND(AVG(calculated_ar), 1) AS avg_ar
        FROM golden_order.fact_encounters
        WHERE weapon_used IS NOT NULL
        GROUP BY weapon_used, upgrade_level, category, archetype
        HAVING COUNT(*) >= 20
        ORDER BY win_rate DESC
    """), get_engine())


@st.cache_data(ttl=30)
def load_archetype_stats():
    return pd.read_sql(text("""
        SELECT archetype,
               ROUND(AVG(strength_stat), 1) AS avg_str,
               ROUND(AVG(dexterity_stat), 1) AS avg_dex,
               ROUND(AVG(intelligence_stat), 1) AS avg_int,
               ROUND(AVG(faith_stat), 1) AS avg_fai,
               ROUND(AVG(arcane_stat), 1) AS avg_arc,
               ROUND(AVG(calculated_ar), 1) AS avg_ar,
               ROUND(100.0 * SUM(CASE WHEN is_victory THEN 1 ELSE 0 END)::numeric / COUNT(*), 1) AS win_rate,
               COUNT(*) AS encounters
        FROM golden_order.fact_encounters
        GROUP BY archetype
        ORDER BY win_rate DESC
    """), get_engine())


def fetch_weapon_row(weapon_name: str, upgrade_level: int) -> pd.Series | None:
    df = pd.read_sql(text("""
        SELECT base_physical, base_magic, base_fire, base_lightning, base_holy,
               scale_str, scale_dex, scale_int, scale_fai, scale_arc, category, weight
        FROM golden_order.dim_weapons
        WHERE weapon_name = :w AND upgrade_level = :u LIMIT 1
    """), get_engine(), params={"w": weapon_name, "u": upgrade_level})
    return df.iloc[0] if not df.empty else None


def compute_ar(row, strength, dexterity, intelligence, faith, arcane, two_handing):
    w = WeaponScaling(
        base_physical=float(row.get("base_physical") or 0),
        base_magic=float(row.get("base_magic") or 0),
        base_fire=float(row.get("base_fire") or 0),
        base_lightning=float(row.get("base_lightning") or 0),
        base_holy=float(row.get("base_holy") or 0),
        scaling={
            "Str": str(row.get("scale_str") or "-"),
            "Dex": str(row.get("scale_dex") or "-"),
            "Int": str(row.get("scale_int") or "-"),
            "Fai": str(row.get("scale_fai") or "-"),
            "Arc": str(row.get("scale_arc") or "-"),
        },
    )
    p = PlayerStats(
        strength=strength, dexterity=dexterity,
        intelligence=intelligence, faith=faith,
        arcane=arcane, two_handing=two_handing,
    )
    return calculate_attack_rating(w, p)


# ── Sidebar nav ───────────────────────────────────────────────────────────────
st.sidebar.title("⚔️ Golden Order")
st.sidebar.markdown("*Elden Ring Combat Analytics*")
page = st.sidebar.radio(
    "Navigate",
    ["Build Advisor", "Boss Gauntlet", "Analytics Dashboard", "Saved Builds"],
)
st.sidebar.markdown("---")
st.sidebar.caption("Data from live Kafka pipeline")

# ══════════════════════════════════════════════════════════════════════════════
# PAGE 1: BUILD ADVISOR
# ══════════════════════════════════════════════════════════════════════════════
if page == "Build Advisor":
    st.title("⚔️ Build Advisor")
    st.markdown("Configure your build and get real-time Attack Rating + soft cap analysis.")

    col1, col2 = st.columns([1, 1])

    with col1:
        st.subheader("Character")
        classes_df = load_classes()
        class_name = st.selectbox("Starting Class", classes_df["class_name"].tolist())
        c = classes_df[classes_df["class_name"] == class_name].iloc[0]

        char_level = st.slider("Character Level", int(c["base_level"]), 150,
                               value=min(80, max(int(c["base_level"]), 80)))
        free_pts = char_level - int(c["base_level"])
        st.caption(f"Free stat points: **{free_pts}**")

        st.subheader("Stats")
        strength     = st.slider("Strength",     int(c["base_strength"]),     99, int(c["base_strength"]))
        dexterity    = st.slider("Dexterity",    int(c["base_dexterity"]),    99, int(c["base_dexterity"]))
        intelligence = st.slider("Intelligence", int(c["base_intelligence"]), 99, int(c["base_intelligence"]))
        faith        = st.slider("Faith",        int(c["base_faith"]),        99, int(c["base_faith"]))
        arcane       = st.slider("Arcane",       int(c["base_arcane"]),       99, int(c["base_arcane"]))
        two_handing  = st.checkbox("Two-handing (+50% effective STR)", value=False)

        spent = (
            (strength     - int(c["base_strength"]))     +
            (dexterity    - int(c["base_dexterity"]))    +
            (intelligence - int(c["base_intelligence"])) +
            (faith        - int(c["base_faith"]))        +
            (arcane       - int(c["base_arcane"]))
        )
        if spent > free_pts:
            st.warning(f"Over budget by {spent - free_pts} points!")
        else:
            st.success(f"Points used: {spent} / {free_pts}")

    with col2:
        st.subheader("Weapon")
        weapons = load_weapons()
        weapon_name = st.selectbox("Weapon", weapons,
                                   index=weapons.index("Rivers of Blood") if "Rivers of Blood" in weapons else 0)

        upgrades = pd.read_sql(text("""
            SELECT DISTINCT upgrade_level FROM golden_order.dim_weapons
            WHERE weapon_name = :w ORDER BY upgrade_level
        """), get_engine(), params={"w": weapon_name})["upgrade_level"].tolist()

        upgrade_level = st.select_slider("Upgrade Level", options=upgrades, value=upgrades[-1])

        row = fetch_weapon_row(weapon_name, upgrade_level)

        if row is not None:
            ar = compute_ar(row, strength, dexterity, intelligence, faith, arcane, two_handing)

            st.subheader("Attack Rating")
            total = ar["total"]
            st.metric("Total AR", f"{total:,.1f}")

            dmg_data = {k: v for k, v in ar.items() if k != "total" and float(v) > 0}
            if dmg_data:
                fig = px.pie(
                    values=list(dmg_data.values()),
                    names=[k.capitalize() for k in dmg_data.keys()],
                    title="Damage Breakdown",
                    color_discrete_sequence=px.colors.sequential.Reds_r,
                )
                fig.update_layout(margin=dict(t=40, b=0, l=0, r=0), height=250)
                st.plotly_chart(fig, use_container_width=True)

            # Scaling grades display
            st.subheader("Weapon Scaling")
            grades = {
                "STR": str(row.get("scale_str") or "-"),
                "DEX": str(row.get("scale_dex") or "-"),
                "INT": str(row.get("scale_int") or "-"),
                "FAI": str(row.get("scale_fai") or "-"),
                "ARC": str(row.get("scale_arc") or "-"),
            }
            gcols = st.columns(5)
            colors = {"S": "🔴", "A": "🟠", "B": "🟡", "C": "🟢", "D": "🔵", "E": "⚪", "-": "⚫"}
            for (stat, grade), col in zip(grades.items(), gcols):
                col.metric(stat, f"{colors.get(grade, '⚫')} {grade}")

    # Soft cap advisor
    if row is not None:
        st.markdown("---")
        st.subheader("Soft Cap Advisor — Where to put your next 5 points")

        stat_fields = [
            ("Strength", strength, "strength"),
            ("Dexterity", dexterity, "dexterity"),
            ("Intelligence", intelligence, "intelligence"),
            ("Faith", faith, "faith"),
            ("Arcane", arcane, "arcane"),
        ]

        gains = []
        for label, val, field in stat_fields:
            kwargs = dict(strength=strength, dexterity=dexterity,
                          intelligence=intelligence, faith=faith,
                          arcane=arcane, two_handing=two_handing)
            kwargs[field] = min(val + 5, 99)
            new_ar = compute_ar(row, **kwargs)["total"]
            gain = new_ar - ar["total"]
            gains.append({"Stat": label, "Current": val, "AR Gain (+5 pts)": round(gain, 1)})

        gains_df = pd.DataFrame(gains).sort_values("AR Gain (+5 pts)", ascending=False)

        fig2 = px.bar(
            gains_df, x="Stat", y="AR Gain (+5 pts)",
            color="AR Gain (+5 pts)",
            color_continuous_scale="RdYlGn",
            title="AR gain from investing 5 more points in each stat",
            text="AR Gain (+5 pts)",
        )
        fig2.update_layout(height=300, margin=dict(t=40, b=0))
        fig2.update_traces(texttemplate="+%{text:.1f}", textposition="outside")
        st.plotly_chart(fig2, use_container_width=True)

        best_stat = gains_df.iloc[0]
        if best_stat["AR Gain (+5 pts)"] > 2:
            st.info(f"**Best investment:** Put next points into **{best_stat['Stat']}** for +{best_stat['AR Gain (+5 pts)']:.1f} AR")
        else:
            st.warning("You've hit soft caps on all stats for this weapon. Consider a different weapon or archetype.")

    # Save build
    st.markdown("---")
    st.subheader("Save Your Build")
    save_col1, save_col2 = st.columns([2, 1])
    with save_col1:
        build_name = st.text_input("Build name", placeholder="e.g. bleed samurai lv80")
    with save_col2:
        if st.button("Save Build", type="primary") and build_name and row is not None:
            ar_val = compute_ar(row, strength, dexterity, intelligence, faith, arcane, two_handing)["total"]
            with get_engine().begin() as conn:
                conn.execute(text("""
                    INSERT INTO golden_order.fact_builds
                        (build_name, class_name, character_level, strength, dexterity,
                         intelligence, faith, arcane, weapon_name, upgrade_level,
                         two_handing, calculated_ar)
                    VALUES (:bn,:cn,:cl,:s,:d,:i,:f,:a,:w,:u,:t,:ar)
                    ON CONFLICT (build_name) DO UPDATE SET
                        class_name=EXCLUDED.class_name, character_level=EXCLUDED.character_level,
                        strength=EXCLUDED.strength, dexterity=EXCLUDED.dexterity,
                        intelligence=EXCLUDED.intelligence, faith=EXCLUDED.faith,
                        arcane=EXCLUDED.arcane, weapon_name=EXCLUDED.weapon_name,
                        upgrade_level=EXCLUDED.upgrade_level, two_handing=EXCLUDED.two_handing,
                        calculated_ar=EXCLUDED.calculated_ar, saved_at=NOW()
                """), dict(bn=build_name, cn=class_name, cl=char_level,
                           s=strength, d=dexterity, i=intelligence,
                           f=faith, a=arcane, w=weapon_name, u=upgrade_level,
                           t=two_handing, ar=ar_val))
            st.success(f"Build '{build_name}' saved! AR: {ar_val:.1f}")

# ══════════════════════════════════════════════════════════════════════════════
# PAGE 2: BOSS GAUNTLET
# ══════════════════════════════════════════════════════════════════════════════
elif page == "Boss Gauntlet":
    st.title("🐉 Boss Gauntlet")
    st.markdown("See how every boss ranks against **your specific build**.")

    gcol1, gcol2 = st.columns([1, 2])

    with gcol1:
        st.subheader("Your Build")
        weapons = load_weapons()
        g_weapon = st.selectbox("Weapon", weapons, key="g_weapon")
        g_upgrades = pd.read_sql(text("""
            SELECT DISTINCT upgrade_level FROM golden_order.dim_weapons
            WHERE weapon_name = :w ORDER BY upgrade_level
        """), get_engine(), params={"w": g_weapon})["upgrade_level"].tolist()
        g_upgrade = st.select_slider("Upgrade", options=g_upgrades, value=g_upgrades[-1], key="g_upg")

        g_str = st.slider("STR", 1, 99, 12, key="g_str")
        g_dex = st.slider("DEX", 1, 99, 40, key="g_dex")
        g_int = st.slider("INT", 1, 99, 9,  key="g_int")
        g_fai = st.slider("FAI", 1, 99, 8,  key="g_fai")
        g_arc = st.slider("ARC", 1, 99, 60, key="g_arc")
        g_2h  = st.checkbox("Two-handing", key="g_2h")

        show_dlc = st.checkbox("Show DLC bosses only", value=False)

    with gcol2:
        row = fetch_weapon_row(g_weapon, g_upgrade)
        if row is not None:
            ar = compute_ar(row, g_str, g_dex, g_int, g_fai, g_arc, g_2h)["total"]
            st.metric("Your AR", f"{ar:,.1f}")

            bosses_df = load_bosses()
            community = load_boss_stats()
            merged = bosses_df.merge(community[["boss_name","death_rate","encounters"]], on="boss_name", how="left")
            merged["death_rate"] = merged["death_rate"].fillna(50.0)
            merged["hits_to_kill"] = (merged["hp"] / ar).round(1)
            merged["difficulty"] = (merged["death_rate"] * 0.7 + merged["hits_to_kill"].clip(upper=100) * 0.3)

            if show_dlc:
                merged = merged[merged["is_dlc"] == True]

            merged = merged.sort_values("difficulty").reset_index(drop=True)
            merged["rank"] = merged.index + 1

            fig = px.bar(
                merged,
                x="difficulty",
                y="boss_name",
                orientation="h",
                color="death_rate",
                color_continuous_scale="RdYlGn_r",
                hover_data={"hp": True, "hits_to_kill": True, "death_rate": True, "encounters": True},
                labels={"difficulty": "Difficulty Score", "boss_name": "Boss",
                        "death_rate": "Death Rate %", "hits_to_kill": "Hits to Kill"},
                title=f"Boss Difficulty for your build ({g_weapon} +{g_upgrade}, AR {ar:.0f})",
                height=max(600, len(merged) * 22),
            )
            fig.update_layout(margin=dict(l=200, t=40), yaxis=dict(autorange="reversed"))
            st.plotly_chart(fig, use_container_width=True)

            st.markdown(f"**Easiest:** {merged.iloc[0]['boss_name']} &nbsp;|&nbsp; **Hardest:** {merged.iloc[-1]['boss_name']}")

# ══════════════════════════════════════════════════════════════════════════════
# PAGE 3: ANALYTICS DASHBOARD
# ══════════════════════════════════════════════════════════════════════════════
elif page == "Analytics Dashboard":
    st.title("📊 Analytics Dashboard")
    st.markdown("Live stats from the streaming pipeline.")

    # Summary metrics
    try:
        totals = pd.read_sql(text("""
            SELECT COUNT(*) AS total_encounters,
                   COUNT(DISTINCT player_id) AS unique_players,
                   ROUND(100.0*SUM(CASE WHEN is_victory THEN 1 ELSE 0 END)::numeric/COUNT(*),1) AS overall_win_rate,
                   ROUND(AVG(calculated_ar),1) AS avg_ar
            FROM golden_order.fact_encounters
        """), get_engine())
        t = totals.iloc[0]
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Total Encounters", f"{int(t['total_encounters']):,}")
        m2.metric("Unique Players",   f"{int(t['unique_players']):,}")
        m3.metric("Overall Win Rate", f"{t['overall_win_rate']}%")
        m4.metric("Avg Attack Rating", f"{t['avg_ar']}")
    except Exception:
        st.warning("No encounter data yet — run the streaming pipeline first.")

    st.markdown("---")
    tab1, tab2, tab3 = st.tabs(["Boss Lethality", "Weapon Win Rates", "Archetype Tier List"])

    with tab1:
        boss_stats = load_boss_stats()
        if not boss_stats.empty:
            top15 = boss_stats.head(15)
            fig = px.bar(top15, x="death_rate", y="boss_name", orientation="h",
                         color="death_rate", color_continuous_scale="Reds",
                         text="death_rate",
                         labels={"death_rate": "Death Rate %", "boss_name": "Boss"},
                         title="Top 15 Most Lethal Bosses")
            fig.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
            fig.update_layout(height=500, yaxis=dict(autorange="reversed"),
                              margin=dict(l=200, t=40))
            st.plotly_chart(fig, use_container_width=True)

            st.dataframe(
                boss_stats[["boss_name","encounters","death_rate","avg_ar","avg_level","is_dlc"]]
                .rename(columns={"boss_name":"Boss","encounters":"Encounters",
                                 "death_rate":"Death Rate %","avg_ar":"Avg AR",
                                 "avg_level":"Avg Level","is_dlc":"DLC"}),
                use_container_width=True, hide_index=True,
            )

    with tab2:
        weapon_stats = load_weapon_stats()
        if not weapon_stats.empty:
            top20 = weapon_stats.head(20)
            fig2 = px.scatter(
                top20, x="avg_ar", y="win_rate", size="uses",
                color="category", hover_name="weapon_used",
                hover_data={"upgrade_level": True, "archetype": True, "uses": True},
                labels={"avg_ar": "Average AR", "win_rate": "Win Rate %"},
                title="Weapon Win Rate vs Average AR (bubble size = usage count)",
                height=500,
            )
            st.plotly_chart(fig2, use_container_width=True)

            st.dataframe(
                top20[["weapon_used","upgrade_level","category","archetype","uses","win_rate","avg_ar"]]
                .rename(columns={"weapon_used":"Weapon","upgrade_level":"Upgrade",
                                 "category":"Category","archetype":"Archetype",
                                 "uses":"Uses","win_rate":"Win Rate %","avg_ar":"Avg AR"}),
                use_container_width=True, hide_index=True,
            )

    with tab3:
        arch_stats = load_archetype_stats()
        if not arch_stats.empty:
            fig3 = px.bar(arch_stats, x="archetype", y="win_rate",
                          color="win_rate", color_continuous_scale="RdYlGn",
                          text="win_rate",
                          labels={"archetype": "Archetype", "win_rate": "Win Rate %"},
                          title="Win Rate by Archetype (Tier List)")
            fig3.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
            fig3.update_layout(height=400)
            st.plotly_chart(fig3, use_container_width=True)

            # Radar chart
            cats = ["avg_str","avg_dex","avg_int","avg_fai","avg_arc"]
            labels = ["STR","DEX","INT","FAI","ARC"]
            fig4 = go.Figure()
            for _, row in arch_stats.iterrows():
                fig4.add_trace(go.Scatterpolar(
                    r=[float(row[c]) for c in cats],
                    theta=labels, fill="toself",
                    name=str(row["archetype"]),
                ))
            fig4.update_layout(
                polar=dict(radialaxis=dict(visible=True, range=[0, 99])),
                title="Average Stat Distribution by Archetype",
                height=450,
            )
            st.plotly_chart(fig4, use_container_width=True)

# ══════════════════════════════════════════════════════════════════════════════
# PAGE 4: SAVED BUILDS
# ══════════════════════════════════════════════════════════════════════════════
elif page == "Saved Builds":
    st.title("📁 Saved Builds")

    try:
        saved = pd.read_sql(text("""
            SELECT build_name, class_name, character_level,
                   strength, dexterity, intelligence, faith, arcane,
                   weapon_name, upgrade_level, two_handing,
                   ROUND(calculated_ar::numeric, 1) AS calculated_ar,
                   saved_at
            FROM golden_order.fact_builds
            ORDER BY calculated_ar DESC
        """), get_engine())
    except Exception:
        saved = pd.DataFrame()

    if saved.empty:
        st.info("No builds saved yet. Go to **Build Advisor** and save a build!")
    else:
        st.dataframe(
            saved.rename(columns={
                "build_name": "Name", "class_name": "Class", "character_level": "Level",
                "strength": "STR", "dexterity": "DEX", "intelligence": "INT",
                "faith": "FAI", "arcane": "ARC", "weapon_name": "Weapon",
                "upgrade_level": "Upgrade", "two_handing": "2H",
                "calculated_ar": "AR", "saved_at": "Saved",
            }),
            use_container_width=True, hide_index=True,
        )

        if len(saved) >= 2:
            st.markdown("---")
            st.subheader("Compare Two Builds")
            cc1, cc2 = st.columns(2)
            names = saved["build_name"].tolist()
            b1_name = cc1.selectbox("Build A", names, key="cmp1")
            b2_name = cc2.selectbox("Build B", [n for n in names if n != b1_name], key="cmp2")

            b1 = saved[saved["build_name"] == b1_name].iloc[0]
            b2 = saved[saved["build_name"] == b2_name].iloc[0]

            # Side-by-side stat comparison
            stats_compare = pd.DataFrame({
                "Stat": ["Level","STR","DEX","INT","FAI","ARC","AR"],
                b1_name: [b1["Level"] if "Level" in b1 else b1["character_level"],
                          b1["STR"] if "STR" in b1 else b1["strength"],
                          b1["DEX"] if "DEX" in b1 else b1["dexterity"],
                          b1["INT"] if "INT" in b1 else b1["intelligence"],
                          b1["FAI"] if "FAI" in b1 else b1["faith"],
                          b1["ARC"] if "ARC" in b1 else b1["arcane"],
                          b1["AR"] if "AR" in b1 else b1["calculated_ar"]],
                b2_name: [b2["Level"] if "Level" in b2 else b2["character_level"],
                          b2["STR"] if "STR" in b2 else b2["strength"],
                          b2["DEX"] if "DEX" in b2 else b2["dexterity"],
                          b2["INT"] if "INT" in b2 else b2["intelligence"],
                          b2["FAI"] if "FAI" in b2 else b2["faith"],
                          b2["ARC"] if "ARC" in b2 else b2["arcane"],
                          b2["AR"] if "AR" in b2 else b2["calculated_ar"]],
            })

            fig = go.Figure()
            fig.add_trace(go.Bar(name=b1_name, x=stats_compare["Stat"],
                                 y=stats_compare[b1_name], marker_color="#ef4444"))
            fig.add_trace(go.Bar(name=b2_name, x=stats_compare["Stat"],
                                 y=stats_compare[b2_name], marker_color="#3b82f6"))
            fig.update_layout(barmode="group", title="Build Comparison",
                              height=400, xaxis_title="", yaxis_title="Value")
            st.plotly_chart(fig, use_container_width=True)

            # Delete build
            st.markdown("---")
            del_name = st.selectbox("Delete a build", names, key="del")
            if st.button("Delete Build", type="secondary"):
                with get_engine().begin() as conn:
                    conn.execute(text("DELETE FROM golden_order.fact_builds WHERE build_name=:n"),
                                 {"n": del_name})
                st.success(f"Deleted '{del_name}'")
                st.rerun()
