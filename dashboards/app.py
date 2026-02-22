"""
StreamPulse Dashboard
Real-time Twitch chat analytics and sentiment visualization.
"""

import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta

# -- Page config --
st.set_page_config(
    page_title="StreamPulse",
    page_icon="📊",
    layout="wide",
)

# -- Database connection --
@st.cache_resource
def get_connection():
    return psycopg2.connect(
        host="localhost",
        port=5432,
        dbname="twitch_chat",
        user="pipeline",
        password="pipeline_password",
    )

def run_query(sql):
    conn = get_connection()
    return pd.read_sql(sql, conn)


# -- Header --
st.title("📊 StreamPulse")
st.markdown("Real-time Twitch chat analytics and sentiment pipeline")
st.divider()


# -- Sidebar filters --
st.sidebar.header("Filters")

channels = run_query("SELECT DISTINCT channel FROM chat_activity ORDER BY channel")
selected_channel = st.sidebar.selectbox(
    "Channel",
    channels["channel"].tolist(),
    index=0,
)


# -- Load data for selected channel --
activity_df = run_query(f"""
    SELECT *
    FROM chat_activity
    WHERE channel = '{selected_channel}'
    ORDER BY minute_bucket
""")

summary_df = run_query(f"""
    SELECT *
    FROM channel_summary
    WHERE channel = '{selected_channel}'
""")


# -- Top-level KPI cards --
if not summary_df.empty:
    row = summary_df.iloc[0]

    col1, col2, col3, col4, col5 = st.columns(5)

    with col1:
        st.metric("Total Messages", f"{int(row['total_messages']):,}")
    with col2:
        st.metric("Unique Chatters", f"{int(row['unique_chatters']):,}")
    with col3:
        avg_sent = row['avg_sentiment']
        emoji = "😊" if avg_sent > 0.05 else "😐" if avg_sent > -0.05 else "😠"
        st.metric("Avg Sentiment", f"{avg_sent:.3f} {emoji}")
    with col4:
        st.metric("Positive Messages", f"{int(row['positive_count']):,}")
    with col5:
        st.metric("Negative Messages", f"{int(row['negative_count']):,}")

    st.divider()


# -- Charts --
if not activity_df.empty:

    # --- Chat Volume Over Time ---
    st.subheader("Chat Volume Over Time")

    vol_fig = go.Figure()
    vol_fig.add_trace(go.Scatter(
        x=activity_df["minute_bucket"],
        y=activity_df["message_count"],
        mode="lines+markers",
        name="Messages/min",
        line=dict(color="#9146FF", width=2),
        marker=dict(size=4),
    ))
    vol_fig.add_trace(go.Scatter(
        x=activity_df["minute_bucket"],
        y=activity_df["unique_chatters"],
        mode="lines+markers",
        name="Unique Chatters",
        line=dict(color="#00C49A", width=2, dash="dash"),
        marker=dict(size=4),
    ))
    vol_fig.update_layout(
        xaxis_title="Time",
        yaxis_title="Count",
        hovermode="x unified",
        template="plotly_dark",
        height=400,
    )
    st.plotly_chart(vol_fig, use_container_width=True)


    # --- Sentiment Over Time ---
    st.subheader("Sentiment Over Time")

    sent_fig = go.Figure()
    sent_fig.add_trace(go.Scatter(
        x=activity_df["minute_bucket"],
        y=activity_df["avg_sentiment"],
        mode="lines+markers",
        name="Avg Sentiment",
        line=dict(color="#FFD700", width=2),
        marker=dict(size=4),
        fill="tozeroy",
        fillcolor="rgba(255, 215, 0, 0.1)",
    ))
    # Add a zero reference line
    sent_fig.add_hline(y=0, line_dash="dot", line_color="gray")
    sent_fig.update_layout(
        xaxis_title="Time",
        yaxis_title="Polarity (-1 to 1)",
        hovermode="x unified",
        template="plotly_dark",
        height=350,
    )
    st.plotly_chart(sent_fig, use_container_width=True)


    # --- Two-column layout for breakdowns ---
    left_col, right_col = st.columns(2)

    with left_col:
        # Sentiment Distribution
        st.subheader("Sentiment Distribution")
        if not summary_df.empty:
            sent_data = pd.DataFrame({
                "Label": ["Positive", "Neutral", "Negative"],
                "Count": [
                    int(row["positive_count"]),
                    int(row["neutral_count"]),
                    int(row["negative_count"]),
                ],
            })
            pie_fig = px.pie(
                sent_data,
                values="Count",
                names="Label",
                color="Label",
                color_discrete_map={
                    "Positive": "#00C49A",
                    "Neutral": "#8884d8",
                    "Negative": "#FF6B6B",
                },
                template="plotly_dark",
            )
            pie_fig.update_layout(height=350)
            st.plotly_chart(pie_fig, use_container_width=True)

    with right_col:
        # Behavior Signals
        st.subheader("Chat Behavior Signals")
        behavior_fig = go.Figure()
        behavior_fig.add_trace(go.Scatter(
            x=activity_df["minute_bucket"],
            y=activity_df["all_caps_ratio"],
            mode="lines",
            name="ALL CAPS Ratio",
            line=dict(color="#FF6B6B", width=2),
        ))
        behavior_fig.add_trace(go.Scatter(
            x=activity_df["minute_bucket"],
            y=activity_df["emote_ratio"],
            mode="lines",
            name="Emote Ratio",
            line=dict(color="#9146FF", width=2),
        ))
        behavior_fig.add_trace(go.Scatter(
            x=activity_df["minute_bucket"],
            y=activity_df["subscriber_ratio"],
            mode="lines",
            name="Subscriber Ratio",
            line=dict(color="#00C49A", width=2),
        ))
        behavior_fig.update_layout(
            xaxis_title="Time",
            yaxis_title="Ratio (0-1)",
            hovermode="x unified",
            template="plotly_dark",
            height=350,
        )
        st.plotly_chart(behavior_fig, use_container_width=True)


    # --- Positive vs Negative Over Time ---
    st.subheader("Positive vs Negative Messages Over Time")

    pn_fig = go.Figure()
    pn_fig.add_trace(go.Bar(
        x=activity_df["minute_bucket"],
        y=activity_df["positive_count"],
        name="Positive",
        marker_color="#00C49A",
    ))
    pn_fig.add_trace(go.Bar(
        x=activity_df["minute_bucket"],
        y=activity_df["negative_count"],
        name="Negative",
        marker_color="#FF6B6B",
    ))
    pn_fig.update_layout(
        barmode="group",
        xaxis_title="Time",
        yaxis_title="Message Count",
        hovermode="x unified",
        template="plotly_dark",
        height=400,
    )
    st.plotly_chart(pn_fig, use_container_width=True)


    # --- Recent Messages Sample ---
    st.subheader("Recent Messages")
    recent = run_query(f"""
        SELECT
            timestamp as time,
            username,
            message,
            sentiment_polarity as sentiment,
            sentiment_label as label
        FROM raw_messages
        WHERE lower(trim(channel)) = '{selected_channel}'
            AND sentiment_label IS NOT NULL
        ORDER BY timestamp DESC
        LIMIT 50
    """)

    if not recent.empty:
        # Color code by sentiment
        def color_label(val):
            if val == "positive":
                return "background-color: #00C49A20"
            elif val == "negative":
                return "background-color: #FF6B6B20"
            return ""

        st.dataframe(
            recent.style.applymap(color_label, subset=["label"]),
            use_container_width=True,
            height=400,
        )

else:
    st.warning("No chat activity data found for this channel. "
               "Run the ingestion pipeline and dbt models first.")


# -- Footer --
st.divider()
st.caption("StreamPulse — Built with Python, dbt, PostgreSQL, and Streamlit")