import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from kafka import KafkaConsumer
import json
import time
from collections import deque

# Page configuration
st.set_page_config(
    page_title="Real-Time Logistics Dashboard",
    page_icon="🚚",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 3rem;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #1f77b4;
    }
    .alert-high {
        background-color: #ffebee;
        border-left: 4px solid #f44336;
        color: #000000;
    }
    .alert-medium {
        background-color: #fff3e0;
        border-left: 4px solid #ff9800;
        color: #000000;
    }
    .alert-low {
        background-color: #e8f5e8;
        border-left: 4px solid #4caf50;
        color: #000000;
    }
</style>
""", unsafe_allow_html=True)

# Initialize session state
if 'data_buffer' not in st.session_state:
    st.session_state.data_buffer = deque(maxlen=100)
if 'total_messages' not in st.session_state:
    st.session_state.total_messages = 0
if 'anomaly_count' not in st.session_state:
    st.session_state.anomaly_count = 0
if 'high_risk_count' not in st.session_state:
    st.session_state.high_risk_count = 0
if 'maintenance_count' not in st.session_state:
    st.session_state.maintenance_count = 0

# Header
st.markdown('<h1 class="main-header">🚚 Real-Time Logistics Dashboard</h1>', unsafe_allow_html=True)

# Sidebar
st.sidebar.title("📊 Dashboard Controls")
update_interval = st.sidebar.slider("Update Interval (seconds)", 1, 10, 2)
show_raw_data = st.sidebar.checkbox("Show Raw Data", False)

# Kafka consumer setup
@st.cache_resource
def get_kafka_consumer():
    try:
        consumer = KafkaConsumer(
            'ml_predictions',
            bootstrap_servers='localhost:9092',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='dashboard-consumer-group',
            consumer_timeout_ms=1000  # 1 second timeout
        )
        return consumer
    except Exception as e:
        st.error(f"Failed to connect to Kafka: {e}")
        return None

# Get messages from Kafka
def get_new_messages():
    consumer = get_kafka_consumer()
    if consumer is None:
        return []
    
    messages = []
    try:
        for message in consumer:
            messages.append(message.value)
    except Exception as e:
        st.warning(f"Kafka consumer error: {e}")
    
    return messages

# Get new messages
new_messages = get_new_messages()

# Process new messages
for data in new_messages:
    st.session_state.data_buffer.append(data)
    st.session_state.total_messages += 1
    
    # Update counters
    if data.get('is_anomaly') == 1:
        st.session_state.anomaly_count += 1
    if data.get('risk_classification_pred') == 'High Risk':
        st.session_state.high_risk_count += 1
    if data.get('maintenance_needed') == 1:
        st.session_state.maintenance_count += 1

# Main content
col1, col2, col3, col4 = st.columns(4)

# Metrics cards
with col1:
    st.metric(
        label="📨 Total Messages",
        value=st.session_state.total_messages,
        delta=None
    )

with col2:
    st.metric(
        label="🚨 Anomalies Detected",
        value=st.session_state.anomaly_count,
        delta=None
    )

with col3:
    st.metric(
        label="⚠️ High Risk Alerts",
        value=st.session_state.high_risk_count,
        delta=None
    )

with col4:
    st.metric(
        label="🔧 Maintenance Needed",
        value=st.session_state.maintenance_count,
        delta=None
    )

# Status indicator
if len(st.session_state.data_buffer) == 0:
    st.warning("⚠️ No data received yet. Make sure the consumer is running and producing to 'ml_predictions' topic.")
else:
    st.success(f"✅ Receiving data: {len(st.session_state.data_buffer)} messages in buffer")

# Create tabs
tab1, tab2, tab3, tab4 = st.tabs(["📈 Real-Time Charts", "🚨 Alerts", "📊 Analytics", "📋 Raw Data"])

with tab1:
    st.subheader("Real-Time Data Visualization")
    
    if len(st.session_state.data_buffer) > 0:
        # Convert buffer to DataFrame
        df = pd.DataFrame(list(st.session_state.data_buffer))
        
        # Create subplots
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=('Anomaly Scores', 'Risk Distribution', 'Maintenance Probability', 'Fuel Consumption'),
            specs=[[{"secondary_y": False}, {"secondary_y": False}],
                   [{"secondary_y": False}, {"secondary_y": False}]]
        )
        
        # Anomaly Scores
        if 'anomaly_score' in df.columns:
            anomaly_scores = df['anomaly_score'].dropna()
            if len(anomaly_scores) > 0:
                fig.add_trace(
                    go.Scatter(y=anomaly_scores, mode='lines+markers', name='Anomaly Score'),
                    row=1, col=1
                )
        
        # Risk Distribution
        if 'risk_classification_pred' in df.columns:
            risk_counts = df['risk_classification_pred'].value_counts()
            if len(risk_counts) > 0:
                fig.add_trace(
                    go.Bar(x=risk_counts.index, y=risk_counts.values, name='Risk Distribution'),
                    row=1, col=2
                )
        
        # Maintenance Probability
        if 'maintenance_risk_prob' in df.columns:
            maintenance_probs = df['maintenance_risk_prob'].dropna()
            if len(maintenance_probs) > 0:
                fig.add_trace(
                    go.Scatter(y=maintenance_probs, mode='lines+markers', name='Maintenance Risk'),
                    row=2, col=1
                )
        
        # Fuel Consumption
        if 'fuel_consumption_rate' in df.columns:
            fuel_data = df['fuel_consumption_rate'].dropna()
            if len(fuel_data) > 0:
                fig.add_trace(
                    go.Scatter(y=fuel_data, mode='lines+markers', name='Fuel Consumption'),
                    row=2, col=2
                )
        
        fig.update_layout(height=600, showlegend=True)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Waiting for data...")

with tab2:
    st.subheader("🚨 Active Alerts")
    
    if len(st.session_state.data_buffer) > 0:
        # Filter recent alerts (last 10 messages)
        recent_data = list(st.session_state.data_buffer)[-10:]
        
        alert_found = False
        for i, data in enumerate(recent_data):
            alert_type = []
            alert_class = "metric-card"
            
            if data.get('is_anomaly') == 1:
                alert_type.append("🚨 Anomaly Detected")
                alert_class += " alert-high"
            
            if data.get('risk_classification_pred') == 'High Risk':
                alert_type.append("⚠️ High Risk")
                alert_class += " alert-high"
            elif data.get('risk_classification_pred') == 'Moderate Risk':
                alert_type.append("⚡ Moderate Risk")
                alert_class += " alert-medium"
            
            if data.get('maintenance_needed') == 1:
                alert_type.append("🔧 Maintenance Required")
                alert_class += " alert-high"
            
            if alert_type:
                alert_found = True
                st.markdown(f"""
                <div class="{alert_class}">
                    <strong>Alert {len(recent_data) - i}</strong><br>
                    {' | '.join(alert_type)}<br>
                    <small>Timestamp: {data.get('timestamp', 'N/A')}</small>
                </div>
                """, unsafe_allow_html=True)
                st.write("---")
        
        if not alert_found:
            st.success("✅ No active alerts - All systems operating normally")
    else:
        st.info("Waiting for data...")

with tab3:
    st.subheader("📊 Analytics Overview")
    
    if len(st.session_state.data_buffer) > 0:
        df = pd.DataFrame(list(st.session_state.data_buffer))
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Risk Classification Distribution")
            if 'risk_classification_pred' in df.columns:
                risk_dist = df['risk_classification_pred'].value_counts()
                if len(risk_dist) > 0:
                    fig = px.pie(values=risk_dist.values, names=risk_dist.index, title="Risk Distribution")
                    st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.subheader("Anomaly Detection Summary")
            if 'is_anomaly' in df.columns:
                anomaly_dist = df['is_anomaly'].value_counts()
                if len(anomaly_dist) > 0:
                    fig = px.bar(x=anomaly_dist.index, y=anomaly_dist.values, 
                               title="Anomaly Detection Results")
                    st.plotly_chart(fig, use_container_width=True)
        
        # Key metrics table
        st.subheader("Key Performance Indicators")
        if len(df) > 0:
            metrics_data = {
                'Metric': ['Average Anomaly Score', 'Average Maintenance Risk', 'High Risk Percentage'],
                'Value': [
                    f"{df['anomaly_score'].mean():.4f}" if 'anomaly_score' in df.columns and df['anomaly_score'].notna().any() else "N/A",
                    f"{df['maintenance_risk_prob'].mean():.4f}" if 'maintenance_risk_prob' in df.columns and df['maintenance_risk_prob'].notna().any() else "N/A",
                    f"{(df['risk_classification_pred'] == 'High Risk').mean() * 100:.1f}%" if 'risk_classification_pred' in df.columns else "N/A"
                ]
            }
            metrics_df = pd.DataFrame(metrics_data)
            st.table(metrics_df)
    else:
        st.info("Waiting for data...")

with tab4:
    st.subheader("📋 Raw Data Stream")
    
    if show_raw_data and len(st.session_state.data_buffer) > 0:
        df = pd.DataFrame(list(st.session_state.data_buffer))
        st.dataframe(df, use_container_width=True)
    elif not show_raw_data:
        st.info("Enable 'Show Raw Data' in the sidebar to view raw data stream")
    else:
        st.info("Waiting for data...")

# Auto-refresh
time.sleep(update_interval)
st.rerun() 