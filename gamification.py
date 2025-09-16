# gamification.py
from __future__ import annotations
import math
import streamlit as st
from streamlit_extras.let_it_rain import rain

BADGE_COPY = {
    "first_10": ("Første 10 sider", "🚀 God start – I er i orbit!"),
    "fifty_percent": ("50% complete", "🧹 Halvvejs gennem greenwash-støvet"),
    "hundred_done": ("100 sider done", "🏆 Vaskemaskinen er tømt"),
}

def celebrate(unlocked: list[str] | None):
    if not unlocked:
        return
    # vis konfetti + toasts
    rain(emoji="🌱", font_size=42, falling_speed=6, animation_length="0")  # én omgang
    for key in unlocked:
        title, desc = BADGE_COPY.get(key, (key, ""))
        st.toast(f"🏅 Badge låst op: {title} — {desc}")

def meter_color(pct: float) -> str:
    if pct >= 0.85: return "#059669"  # grøn
    if pct >= 0.6:  return "#10b981"  # lysegrøn
    if pct >= 0.35: return "#f59e0b"  # gul
    return "#ef4444"                  # rød

def greenwash_meter(completion_pct: float):
    c = meter_color(completion_pct)
    nice = int(round(completion_pct * 100))
    quips = [
        "🧽 Der skrubbes løs…",
        "🔍 Greenwash-detektor kalibreres…",
        "🪣 Næsten rent vand!",
        "🌈 Ren samvittighed i sigte!",
    ]
    joke = quips[min(3, math.floor(completion_pct * 4))]
    st.markdown(
        f"""
        <div style="border-radius:12px;padding:14px 16px;background:linear-gradient(90deg,{c} {nice}%,#e5e7eb {nice}%);color:#111;">
          <b>Greenwash-o-meter:</b> {nice}% &nbsp; {joke}
        </div>
        """,
        unsafe_allow_html=True,
    )

def badge_strip(stats: dict, unlocked_names: list[str] | None = None):
    total = stats.get("total", 0)
    done = stats.get("done", 0)
    pct  = stats.get("completion", 0.0)
    st.markdown("#### 🏅 Badges")
    cols = st.columns(3)
    items = [
        ("first_10", f"{done}/10"),
        ("fifty_percent", f"{int(pct*100)}%"),
        ("hundred_done", f"{done}/100"),
    ]
    for i, (key, progress) in enumerate(items):
        title, desc = BADGE_COPY.get(key, (key, ""))
        active = (unlocked_names and key in unlocked_names)
        border = "2px solid #059669" if active else "1px solid #e5e7eb"
        cols[i].markdown(
            f"""
            <div style="border:{border};border-radius:12px;padding:12px;">
              <div style="font-size:18px;">🏅 {title}</div>
              <div style="color:#6b7280;font-size:13px;">{desc}</div>
              <div style="margin-top:6px;background:#f3f4f6;border-radius:8px;padding:6px 8px;display:inline-block;">
                {progress}
              </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

def daily_quest(done_today: int, target: int = 5):
    left = max(0, target - done_today)
    status = "✅ Klaret!" if left == 0 else f"⏳ {left} tilbage"
    st.markdown("#### ⚔️ Dagens quest")
    st.info(f"Gør **{target}** sider færdige i dag. {status}")

def panel(stats: dict, unlocked_now: list[str] | None, done_today: int):
    st.markdown("### 🎮 Gamification")
    greenwash_meter(stats.get("completion", 0.0))
    badge_strip(stats, unlocked_now)
    daily_quest(done_today)
    celebrate(unlocked_now)  # kør til sidst, så toasts ses
