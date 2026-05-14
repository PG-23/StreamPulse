"""
StreamPulse Dashboard

Interactive Streamlit application for exploring Twitch chat analytics
and sentiment data produced by the StreamPulse pipeline.

Run with:
    streamlit run app.py
"""

import logging

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import psycopg2
import streamlit as st

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SENTIMENT_POSITIVE_THRESHOLD = 0.05
SENTIMENT_NEGATIVE_THRESHOLD = -0.05

COLOR_POSITIVE = "#00C49A"
COLOR_NEGATIVE = "#FF6B6B"
COLOR_NEUTRAL = "#8884d8"
COLOR_TWITCH = "#9146FF"
COLOR_GOLD = "#FFD700"

RECENT_MESSAGES_LIMIT = 50

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="StreamPulse",
    page_icon="📊",
    layout="wide",
)

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

@st.cache_resource
def get_connection() -> psycopg2.extensions.connection:
    """
    Create and cache a PostgreSQL connection for the lifetime of the app.

    Uses st.cache_resource so the connection is shared across reruns
    rather than reopened on every interaction.

    Returns:
        An open psycopg2 connection.
    """
    log.info("Opening database connection.")
    return psycopg2.connect(
        host="localhost",
        port=5432,
        dbname="twitch_chat",
        user="pipeline",
        password="pipeline_password",
    )


@st.cache_data(ttl=30)
def run_query(sql: str) -> pd.DataFrame:
    """
    Execute a SQL query and return results as a DataFrame.

    Results are cached for 30 seconds (ttl) so repeated interactions
    do not hammer the database on every Streamlit rerun.

    Args:
        sql: SQL string to execute.

    Returns:
        A pandas DataFrame containing the query results.
    """
    conn = get_connection()
    return pd.read_sql(sql, conn)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def sentiment_emoji(avg_sentiment: float) -> str:
    """
    Return an emoji representing the average sentiment score.

    Args:
        avg_sentiment: Float polarity value in range [-1.0, 1.0].

    Returns:
        A single emoji string.
    """
    if avg_sentiment > SENTIMENT_POSITIVE_THRESHOLD:
        return "😊"
    elif avg_sentiment > SENTIMENT_NEGATIVE_THRESHOLD:
        return "😐"
    return "😠"


def color_sentiment_label(val: str) -> str:
    """
    Return a CSS background-color style string for a sentiment label.

    Used with DataFrame.style.applymap() to highlight rows by sentiment.

    Args:
        val: One of 'positive', 'negative', or 'neutral'.

    Returns:
        A CSS background-color string.
    """
    if val == "positive":
        return f"background-color: {COLOR_POSITIVE}20"
    elif val == "negative":
        return f"background-color: {COLOR_NEGATIVE}20"
    return ""


# ---------------------------------------------------------------------------
# Layout
# ---------------------------------------------------------------------------

st.title("📊 StreamPulse")
st.markdown("Real-time Twitch chat analytics and sentiment pipeline")
st.divider()

# Sidebar filters
st.sidebar.header("Filters")
channels = run_query("SELECT DISTINCT channel FROM chat_activity ORDER BY channel")
selected_channel = st.sidebar.selectbox(
    "Channel",
    channels["channel"].tolist(),
    index=0,
)

# Load data for selected channel
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

# ---------------------------------------------------------------------------
# KPI cards
# ---------------------------------------------------------------------------

if not summary_df.empty:
    row = summary_df.iloc[0]
    avg_sent = row["avg_sentiment"]

    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        st.metric("Total Messages", f"{int(row['total_messages']):,}")
    with col2:
        st.metric("Unique Chatters", f"{int(row['unique_chatters']):,}")
    with col3:
        st.metric("Avg Sentiment", f"{avg_sent:.3f} {sentiment_emoji(avg_sent)}")
    with col4:
        st.metric("Positive Messages", f"{int(row['positive_count']):,}")
    with col5:
        st.metric("Negative Messages", f"{int(row['negative_count']):,}")

    st.divider()

# ---------------------------------------------------------------------------
# Charts
# ---------------------------------------------------------------------------

if not activity_df.empty:

    # Chat volume over time
    st.subheader("Chat Volume Over Time")
    vol_fig = go.Figure()
    vol_fig.add_trace(go.Scatter(
        x=activity_df["minute_bucket"],
        y=activity_df["message_count"],
        mode="lines+markers",
        name="Messages/min",
        line=dict(color=COLOR_TWITCH, width=2),
        marker=dict(size=4),
    ))
    vol_fig.add_trace(go.Scatter(
        x=activity_df["minute_bucket"],
        y=activity_df["unique_chatters"],
        mode="lines+markers",
        name="Unique Chatters",
        line=dict(color=COLOR_POSITIVE, width=2, dash="dash"),
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

    # Sentiment over time
    st.subheader("Sentiment Over Time")
    sent_fig = go.Figure()
    sent_fig.add_trace(go.Scatter(
        x=activity_df["minute_bucket"],
        y=activity_df["avg_sentiment"],
        mode="lines+markers",
        name="Avg Sentiment",
        line=dict(color=COLOR_GOLD, width=2),
        marker=dict(size=4),
        fill="tozeroy",
        fillcolor="rgba(255, 215, 0, 0.1)",
    ))
    sent_fig.add_hline(y=0, line_dash="dot", line_color="gray")
    sent_fig.update_layout(
        xaxis_title="Time",
        yaxis_title="Polarity (-1 to 1)",
        hovermode="x unified",
        template="plotly_dark",
        height=350,
    )
    st.plotly_chart(sent_fig, use_container_width=True)

    # Sentiment distribution + behavior signals
    left_col, right_col = st.columns(2)

    with left_col:
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
                    "Positive": COLOR_POSITIVE,
                    "Neutral": COLOR_NEUTRAL,
                    "Negative": COLOR_NEGATIVE,
                },
                template="plotly_dark",
            )
            pie_fig.update_layout(height=350)
            st.plotly_chart(pie_fig, use_container_width=True)

    with right_col:
        st.subheader("Chat Behavior Signals")
        behavior_fig = go.Figure()
        behavior_fig.add_trace(go.Scatter(
            x=activity_df["minute_bucket"],
            y=activity_df["all_caps_ratio"],
            mode="lines",
            name="ALL CAPS Ratio",
            line=dict(color=COLOR_NEGATIVE, width=2),
        ))
        behavior_fig.add_trace(go.Scatter(
            x=activity_df["minute_bucket"],
            y=activity_df["emote_ratio"],
            mode="lines",
            name="Emote Ratio",
            line=dict(color=COLOR_TWITCH, width=2),
        ))
        behavior_fig.add_trace(go.Scatter(
            x=activity_df["minute_bucket"],
            y=activity_df["subscriber_ratio"],
            mode="lines",
            name="Subscriber Ratio",
            line=dict(color=COLOR_POSITIVE, width=2),
        ))
        behavior_fig.update_layout(
            xaxis_title="Time",
            yaxis_title="Ratio (0-1)",
            hovermode="x unified",
            template="plotly_dark",
            height=350,
        )
        st.plotly_chart(behavior_fig, use_container_width=True)

    # Positive vs negative over time
    st.subheader("Positive vs Negative Messages Over Time")
    pn_fig = go.Figure()
    pn_fig.add_trace(go.Bar(
        x=activity_df["minute_bucket"],
        y=activity_df["positive_count"],
        name="Positive",
        marker_color=COLOR_POSITIVE,
    ))
    pn_fig.add_trace(go.Bar(
        x=activity_df["minute_bucket"],
        y=activity_df["negative_count"],
        name="Negative",
        marker_color=COLOR_NEGATIVE,
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

    # Recent messages table
    st.subheader("Recent Messages")
    recent = run_query(f"""
        SELECT
            timestamp AS time,
            username,
            message,
            sentiment_polarity AS sentiment,
            sentiment_label AS label
        FROM raw_messages
        WHERE lower(trim(channel)) = '{selected_channel}'
            AND sentiment_label IS NOT NULL
        ORDER BY timestamp DESC
        LIMIT {RECENT_MESSAGES_LIMIT}
    """)

    if not recent.empty:
        st.dataframe(
            recent.style.applymap(color_sentiment_label, subset=["label"]),
            use_container_width=True,
            height=400,
        )

else:
    st.warning(
        "No chat activity data found for this channel. "
        "Run the ingestion pipeline and dbt models first."
    )

# ---------------------------------------------------------------------------
# Footer
# ---------------------------------------------------------------------------

st.divider()
st.caption("StreamPulse — Built with Python, dbt, PostgreSQL, and Streamlit")