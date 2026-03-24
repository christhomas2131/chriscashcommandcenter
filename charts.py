from datetime import datetime

import pandas as pd
import plotly.graph_objects as go

STATUS_COLORS = {
    "Researching": "#6B7280",
    "Ready to Apply": "#3B82F6",
    "Applied": "#60A5FA",
    "Phone Screen": "#F59E0B",
    "Interview": "#F97316",
    "Technical Assessment": "#FB923C",
    "Final Round": "#A855F7",
    "Offer": "#10B981",
    "Rejected": "#DC2626",
    "Withdrawn": "#9CA3AF",
    "Ghosted": "#4B5563",
}

_LAYOUT_BASE = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(color="#E5E7EB"),
)

_MARGIN_DEFAULT = dict(l=0, r=0, t=20, b=0)


def _axis(gridcolor="#374151", tick_color="#9CA3AF", **kwargs):
    return dict(gridcolor=gridcolor, tickfont=dict(color=tick_color), **kwargs)


def applications_over_time_chart(data):
    if not data:
        fig = go.Figure()
        fig.update_layout(**_LAYOUT_BASE, height=200)
        return fig

    df = pd.DataFrame(data)

    def week_to_label(w):
        try:
            return datetime.strptime(w + "-1", "%Y-%W-%w").strftime("%b %d")
        except Exception:
            return w

    df["label"] = df["week"].apply(week_to_label)

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df["label"],
        y=df["count"],
        mode="lines+markers",
        fill="tozeroy",
        line=dict(color="#3B82F6", width=2),
        fillcolor="rgba(59,130,246,0.12)",
        marker=dict(size=8, color="#3B82F6"),
        hovertemplate="%{x}: <b>%{y}</b> applications<extra></extra>",
    ))
    fig.update_layout(
        **_LAYOUT_BASE,
        height=240,
        margin=_MARGIN_DEFAULT,
        xaxis=_axis(),
        yaxis=_axis(title=dict(text="Applications", font=dict(color="#9CA3AF"))),
    )
    return fig


def status_breakdown_chart(data):
    if not data:
        fig = go.Figure()
        fig.update_layout(**_LAYOUT_BASE, height=200)
        return fig

    df = pd.DataFrame(data)
    colors = [STATUS_COLORS.get(s, "#6B7280") for s in df["status"]]

    fig = go.Figure(go.Bar(
        x=df["count"],
        y=df["status"],
        orientation="h",
        marker_color=colors,
        text=df["count"],
        textposition="outside",
        textfont=dict(color="#E5E7EB"),
        hovertemplate="%{y}: <b>%{x}</b><extra></extra>",
    ))
    fig.update_layout(
        **_LAYOUT_BASE,
        height=max(180, len(df) * 36),
        xaxis=_axis(),
        yaxis=dict(tickfont=dict(color="#E5E7EB")),
        margin=dict(l=0, r=50, t=20, b=0),
    )
    return fig


def funnel_chart(status_counts):
    ORDER = [
        "Researching", "Ready to Apply", "Applied",
        "Phone Screen", "Interview", "Technical Assessment",
        "Final Round", "Offer",
    ]
    count_map = {d["status"]: d["count"] for d in status_counts}
    stages = [s for s in ORDER if count_map.get(s, 0) > 0]
    counts = [count_map[s] for s in stages]

    if not stages:
        fig = go.Figure()
        fig.update_layout(**_LAYOUT_BASE, height=300)
        return fig

    fig = go.Figure(go.Funnel(
        y=stages,
        x=counts,
        textinfo="value+percent initial",
        marker=dict(color=[STATUS_COLORS.get(s, "#6B7280") for s in stages]),
        textfont=dict(color="white"),
        connector=dict(fillcolor="#1F2937"),
        hovertemplate="%{y}: <b>%{x}</b><extra></extra>",
    ))
    fig.update_layout(**_LAYOUT_BASE, height=380, margin=_MARGIN_DEFAULT)
    return fig


def source_chart(data):
    if not data:
        fig = go.Figure()
        fig.update_layout(**_LAYOUT_BASE, height=260)
        return fig

    df = pd.DataFrame(data)
    fig = go.Figure()
    fig.add_trace(go.Bar(
        name="Total", x=df["source"], y=df["total"],
        marker_color="#3B82F6",
        hovertemplate="%{x} total: <b>%{y}</b><extra></extra>",
    ))
    fig.add_trace(go.Bar(
        name="Interviews", x=df["source"], y=df["interviews"],
        marker_color="#F97316",
        hovertemplate="%{x} interviews: <b>%{y}</b><extra></extra>",
    ))
    fig.add_trace(go.Bar(
        name="Offers", x=df["source"], y=df["offers"],
        marker_color="#10B981",
        hovertemplate="%{x} offers: <b>%{y}</b><extra></extra>",
    ))
    fig.update_layout(
        **_LAYOUT_BASE,
        barmode="group",
        height=300,
        margin=_MARGIN_DEFAULT,
        xaxis=_axis(),
        yaxis=_axis(),
        legend=dict(font=dict(color="#E5E7EB"), bgcolor="rgba(0,0,0,0)"),
    )
    return fig


def salary_distribution_chart(jobs):
    entries = [
        (j["salary_min"], j["salary_max"], j["company_name"])
        for j in jobs
        if j.get("salary_min") or j.get("salary_max")
    ]
    if not entries:
        return None

    fig = go.Figure()
    for low, high, company in entries:
        low = low or high
        high = high or low
        fig.add_trace(go.Scatter(
            x=[low / 1000, high / 1000],
            y=[company, company],
            mode="lines+markers",
            line=dict(color="#3B82F6", width=3),
            marker=dict(size=10, color=["#60A5FA", "#10B981"]),
            showlegend=False,
            hovertemplate=f"{company}: $%{{x}}K<extra></extra>",
        ))
    fig.update_layout(
        **_LAYOUT_BASE,
        height=max(180, len(entries) * 45),
        xaxis=_axis(title=dict(text="Salary (K)", font=dict(color="#9CA3AF"))),
        yaxis=dict(tickfont=dict(color="#E5E7EB")),
        margin=dict(l=0, r=20, t=20, b=0),
    )
    return fig


def response_time_chart(data):
    """Avg days from application to first response, by source."""
    if not data:
        return None

    sources  = [d["source"] or "Unknown" for d in data]
    avg_days = [round(d["avg_days"], 1)   for d in data]
    counts   = [d["count"]                for d in data]

    fig = go.Figure(go.Bar(
        x=avg_days,
        y=sources,
        orientation="h",
        text=[f"{d:.1f}d  (n={c})" for d, c in zip(avg_days, counts)],
        textposition="outside",
        textfont=dict(color="#E5E7EB"),
        marker_color="#A855F7",
        hovertemplate="%{y}: <b>%{x:.1f}</b> days avg<extra></extra>",
    ))
    fig.update_layout(
        **_LAYOUT_BASE,
        height=max(180, len(data) * 44),
        xaxis=_axis(title=dict(text="Avg Days to First Response", font=dict(color="#9CA3AF"))),
        yaxis=dict(tickfont=dict(color="#E5E7EB")),
        margin=dict(l=0, r=90, t=20, b=0),
    )
    return fig


def avg_time_chart(stage_times):
    if not stage_times:
        fig = go.Figure()
        fig.update_layout(**_LAYOUT_BASE, height=200)
        return fig

    df = pd.DataFrame(stage_times)
    df = df[df["avg_days"].notna()].sort_values("avg_days")

    fig = go.Figure(go.Bar(
        x=df["avg_days"].round(1),
        y=df["status"],
        orientation="h",
        marker_color=[STATUS_COLORS.get(s, "#6B7280") for s in df["status"]],
        text=df["avg_days"].round(1).astype(str) + " d",
        textposition="outside",
        textfont=dict(color="#E5E7EB"),
        hovertemplate="%{y}: <b>%{x:.1f}</b> days avg<extra></extra>",
    ))
    fig.update_layout(
        **_LAYOUT_BASE,
        height=max(180, len(df) * 36),
        xaxis=_axis(title=dict(text="Avg Days", font=dict(color="#9CA3AF"))),
        yaxis=dict(tickfont=dict(color="#E5E7EB")),
        margin=dict(l=0, r=60, t=20, b=0),
    )
    return fig
