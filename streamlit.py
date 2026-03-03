import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
from sklearn.preprocessing import MinMaxScaler

st.set_page_config(page_title="Airline Health Dashboard", layout="wide")
st.title("✈️ Airline Health Dashboard")

# ── Data loader ────────────────────────────────────────────────────────────────
@st.cache_data
def load_data(uploaded_file):
    df = pd.read_csv(uploaded_file, low_memory=False)
    df.columns = df.columns.str.strip()
    df["FL_DATE"] = pd.to_datetime(df["FL_DATE"], errors="coerce")
    for col in ["DEP_DELAY", "ARR_DELAY", "CANCELLED", "DEP_DEL15"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    df["OP_CARRIER"] = df["OP_CARRIER"].str.strip()
    return df

# ── Health score ───────────────────────────────────────────────────────────────
def compute_health_scores(df, weights):
    grp = df.groupby("OP_CARRIER").agg(
        total_flights  = ("FL_DATE",   "count"),
        on_time_pct    = ("DEP_DEL15", lambda x: 1 - x.mean()),
        cancel_rate    = ("CANCELLED", "mean"),
        avg_delay      = ("DEP_DELAY", lambda x: x.clip(lower=0).mean()),
        recovery_score = ("ARR_DELAY", lambda x: (df.loc[x.index, "DEP_DELAY"] - x).clip(lower=0).mean()),
    ).reset_index()

    grp["cancel_score"] = 1 - grp["cancel_rate"]

    scaler = MinMaxScaler()
    grp["on_time_norm"]  = scaler.fit_transform(grp[["on_time_pct"]])
    grp["cancel_norm"]   = scaler.fit_transform(grp[["cancel_score"]])
    grp["delay_norm"]    = 1 - scaler.fit_transform(grp[["avg_delay"]])
    grp["recovery_norm"] = scaler.fit_transform(grp[["recovery_score"]])

    total_w = sum(weights.values())
    grp["health_score"] = (
        grp["on_time_norm"]  * weights["on_time"]   / total_w +
        grp["cancel_norm"]   * weights["cancel"]    / total_w +
        grp["delay_norm"]    * weights["delay"]     / total_w +
        grp["recovery_norm"] * weights["recovery"]  / total_w
    ) * 100

    return grp

# ── Sidebar ────────────────────────────────────────────────────────────────────
with st.sidebar:
    uploaded = st.file_uploader("Upload BTS On-Time CSV", type=["csv"])

    st.markdown("### Score Weights")
    weights = {
        "on_time":  st.slider("On-Time Rate",        0.0, 1.0, 0.35, 0.05),
        "cancel":   st.slider("Cancellation Rate",   0.0, 1.0, 0.25, 0.05),
        "delay":    st.slider("Avg Departure Delay", 0.0, 1.0, 0.25, 0.05),
        "recovery": st.slider("Delay Recovery",      0.0, 1.0, 0.15, 0.05),
    }

if not uploaded:
    st.info("Upload a BTS On-Time Performance CSV from the sidebar to get started.")
    st.stop()

# ── Load & score ───────────────────────────────────────────────────────────────
df = load_data(uploaded)
stats = compute_health_scores(df, weights).sort_values("health_score", ascending=False)

# ── Fleet health bar chart ─────────────────────────────────────────────────────
st.subheader("Fleet Health Scores")

fig = px.bar(
    stats.sort_values("health_score"),
    x="health_score", y="OP_CARRIER", orientation="h",
    color="health_score",
    color_continuous_scale=["#e05c5c", "#f0c040", "#4caf80"],
    range_color=[0, 100],
    text=stats.sort_values("health_score")["health_score"].round(1),
    labels={"health_score": "Health Score", "OP_CARRIER": "Carrier"},
)
fig.update_traces(textposition="outside")
fig.update_coloraxes(showscale=False)
fig.update_layout(height=max(300, len(stats) * 40), margin=dict(l=10, r=40, t=20, b=20))
st.plotly_chart(fig, use_container_width=True)

st.divider()

# ── Carrier drill-down ─────────────────────────────────────────────────────────
st.subheader("Carrier Detail")

selected = st.selectbox(
    "Select a carrier",
    options=stats["OP_CARRIER"].tolist(),
    format_func=lambda x: f"{x}  —  Score: {stats.loc[stats['OP_CARRIER']==x, 'health_score'].values[0]:.1f}"
)

row = stats[stats["OP_CARRIER"] == selected].iloc[0]
score = row["health_score"]
color = "#4caf80" if score >= 70 else "#f0c040" if score >= 45 else "#e05c5c"

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Health Score",  f"{score:.1f} / 100")
c2.metric("Total Flights", f"{int(row['total_flights']):,}")
c3.metric("On-Time Rate",  f"{row['on_time_pct']*100:.1f}%")
c4.metric("Cancel Rate",   f"{row['cancel_rate']*100:.2f}%")
c5.metric("Avg Dep Delay", f"{row['avg_delay']:.1f} min")

# Weekly on-time trend
carrier_df = df[df["OP_CARRIER"] == selected].copy()
weekly = (
    carrier_df.set_index("FL_DATE")
    .resample("W")["DEP_DEL15"]
    .apply(lambda x: (1 - x.mean()) * 100)
    .reset_index()
    .rename(columns={"DEP_DEL15": "on_time_pct"})
)

fig2 = px.line(
    weekly, x="FL_DATE", y="on_time_pct",
    title=f"{selected} — Weekly On-Time Rate",
    labels={"FL_DATE": "Week", "on_time_pct": "On-Time %"},
)
fig2.update_traces(line_color=color, line_width=2)
fig2.update_layout(margin=dict(l=10, r=10, t=40, b=20))
st.plotly_chart(fig2, use_container_width=True)