---

# Real-Time Logistics ML Pipeline

This project demonstrates a real-time logistics monitoring and predictive maintenance system using Kafka, machine learning, and a live dashboard.

## Features

- **Real-time data streaming** with Apache Kafka
- **Anomaly detection** and **risk classification** using ML models
- **Predictive maintenance** with LSTM
- **Live dashboard** for monitoring alerts, analytics, and raw data (Streamlit)

---

## Project Structure

```
Real-Time Logistics/
├── consumer/
│   └── consumer_infer.py
├── dashboard/
│   └── dashboard.py
├── models/
│   ├── lstm_maintenance_model.h5
│   ├── lstm_maintenance_scaler.save
│   ├── risk_classifier_xgb.pkl
│   ├── risk_label_encoder.save
│   ├── risk_scaler.save
│   ├── supply_chain_autoencoder.h5
│   └── supply_chain_scaler.pkl
├── produce/
│   └── producer.py
├── supply_chain.csv
├── requirements.txt
├── train_models.ipynb
└── readme.md
```

---

## Setup Instructions

### 1. **Install Requirements**

```sh
pip install -r requirements.txt
```

### 2. **Start Kafka**

Make sure you have Kafka running locally on `localhost:9092`.  
Create the required topics:

```sh
# Create logistics_stream topic
bin\windows\kafka-topics.bat --create --topic logistics_stream --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

# Create ml_predictions topic
bin\windows\kafka-topics.bat --create --topic ml_predictions --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

### 3. **Run the Pipeline**

Open three terminals in the project root:

**Terminal 1: Start the Producer**
```sh
python produce/producer.py
```

**Terminal 2: Start the Consumer/Inferencer**
```sh
python consumer/consumer_infer.py
```

**Terminal 3: Start the Dashboard**
```sh
streamlit run dashboard/dashboard.py
```

Open the Streamlit URL in your browser to view the dashboard.

---

## Notes

- Ensure all model files are present in the `models/` directory.
- The data file `supply_chain.csv` should be in the project root.
- If you need to retrain models, use `train_models.ipynb`.

---

## License

[MIT](LICENSE) (or your preferred license)

---

Let me know if you want a section for **model training**, **troubleshooting**, or anything else!