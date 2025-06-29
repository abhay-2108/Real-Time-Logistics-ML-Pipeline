from kafka import KafkaConsumer, KafkaProducer
import json
import numpy as np
import pandas as pd
import joblib
from keras.models import load_model
from math import radians, cos, sin, sqrt, atan2

# 🔁 Load models
print("🔁 Loading ML models...")
autoencoder = load_model("../models/supply_chain_autoencoder.h5", compile=False)
ae_scaler = joblib.load("../models/supply_chain_scaler.pkl")

xgb = joblib.load("../models/risk_classifier_xgb.pkl")
xgb_scaler = joblib.load("../models/risk_scaler.save")
label_encoder = joblib.load("../models/risk_label_encoder.save")

lstm_model = load_model("../models/lstm_maintenance_model.h5", compile=False)
lstm_scaler = joblib.load("../models/lstm_maintenance_scaler.save")

# ✅ Kafka setup
consumer = KafkaConsumer(
    'logistics_stream',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id='logistics-consumer-group'
)

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# 📌 Features
features = [
    'eta_variation_hours', 'fuel_consumption_rate',
    'traffic_congestion_level', 'supplier_reliability_score',
    'route_risk_level', 'customs_clearance_time'
]

features_pm = [
    'fuel_consumption_rate', 'iot_temperature',
    'driver_behavior_score', 'fatigue_monitoring_score',
    'cargo_condition_status', 'estimated_mileage',
    'loading_unloading_time', 'traffic_congestion_level',
    'weather_condition_severity', 'port_congestion_level'
]

threshold = 0.05  # AE threshold

# Global variables for mileage tracking
last_lat = None
last_lon = None
cumulative_mileage = 0.0

def haversine(lat1, lon1, lat2, lon2):
    """Calculate distance between two GPS points in miles"""
    R = 3959  # Earth radius in miles
    
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    
    a = sin(dlat/2)**2 + cos(lat1)*cos(lat2)*sin(dlon/2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    
    return R * c

def calculate_estimated_mileage(lat, lon):
    """Calculate estimated mileage based on GPS coordinates"""
    global last_lat, last_lon, cumulative_mileage
    
    if last_lat is not None and last_lon is not None:
        distance = haversine(last_lat, last_lon, lat, lon)
        cumulative_mileage += distance
    
    last_lat = lat
    last_lon = lon
    
    return cumulative_mileage

print("✅ Kafka Consumer started. Listening to 'logistics_stream'...\n")

for i, message in enumerate(consumer):
    data = message.value

    try:
        # ✅ Calculate estimated_mileage if GPS coordinates are available
        if 'vehicle_gps_latitude' in data and 'vehicle_gps_longitude' in data:
            data['estimated_mileage'] = calculate_estimated_mileage(
                data['vehicle_gps_latitude'], 
                data['vehicle_gps_longitude']
            )
        else:
            # Use a default value if GPS not available
            data['estimated_mileage'] = cumulative_mileage

        # ✅ Anomaly Detection + Risk Classification
        if all(f in data for f in features):
            # Create DataFrame with proper feature names for scaler
            input_df = pd.DataFrame([{f: data[f] for f in features}])
            x_scaled = ae_scaler.transform(input_df)

            recon = autoencoder.predict(x_scaled, verbose=0)
            loss = np.mean((x_scaled - recon) ** 2)
            is_anomaly = int(loss > threshold)

            data['anomaly_score'] = round(float(loss), 6)
            data['is_anomaly'] = is_anomaly

            clf_scaled = xgb_scaler.transform(input_df)
            pred = xgb.predict(clf_scaled)[0]
            risk = label_encoder.inverse_transform([pred])[0]
            data['risk_classification_pred'] = risk
        else:
            missing_features = [f for f in features if f not in data]
            print(f"⚠️ Missing features for anomaly/risk classification in msg {i+1}: {missing_features}")
            data['anomaly_score'] = None
            data['is_anomaly'] = None
            data['risk_classification_pred'] = None

        # ✅ Predictive Maintenance
        if all(f in data for f in features_pm):
            # Create DataFrame with proper feature names for scaler
            input_dict = {f: data[f] for f in features_pm}
            input_df = pd.DataFrame([input_dict])

            x_scaled_pm = lstm_scaler.transform(input_df)
            
            # LSTM expects 3D input (batch_size, time_steps, features)
            # Since we're doing real-time prediction, we'll use a single time step
            x_seq = np.expand_dims(x_scaled_pm, axis=0)  # (1, 1, features)

            prob = lstm_model.predict(x_seq, verbose=0)[0][0]
            maintenance_needed = int(prob > 0.5)

            data['maintenance_risk_prob'] = round(float(prob), 6)
            data['maintenance_needed'] = maintenance_needed
        else:
            missing_features = [f for f in features_pm if f not in data]
            print(f"⚠️ Missing features for maintenance prediction in msg {i+1}: {missing_features}")
            data['maintenance_risk_prob'] = None
            data['maintenance_needed'] = None

        # ✅ Publish result
        producer.send('ml_predictions', value=data)
        print(f"✅ [{i+1}] Sent → Anomaly: {data.get('is_anomaly')} | "
              f"Risk: {data.get('risk_classification_pred')} | "
              f"Maintenance: {data.get('maintenance_needed')}")

    except Exception as e:
        print(f"❌ Error in message {i+1}: {e}")
        import traceback
        traceback.print_exc()
