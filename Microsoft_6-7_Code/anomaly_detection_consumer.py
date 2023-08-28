from azure.ai.anomalydetector import AnomalyDetectorClient
from azure.ai.anomalydetector.models import TimeSeriesPoint, UnivariateDetectionOptions
from azure.core.credentials import AzureKeyCredential
from confluent_kafka import Consumer, KafkaError

# Azure Anomaly Detector credentials
SUBSCRIPTION_KEY = os.environ["ANOMALY_DETECTOR_KEY"]
ANOMALY_DETECTOR_ENDPOINT = os.environ["ANOMALY_DETECTOR_ENDPOINT"]
anomaly_detector_client = AnomalyDetectorClient(ANOMALY_DETECTOR_ENDPOINT, AzureKeyCredential(SUBSCRIPTION_KEY))

conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'anomaly-detection-group',
    'auto.offset.reset': 'latest' 
}

consumer = Consumer(conf)
consumer.subscribe(['aws-cloudwatch-network'])

def detect_anomalies(data):
    request = UnivariateDetectionOptions(
                    series=data,
                    granularity='minutely',
                    custom_interval=5
                )
    try:
        response = anomaly_detector_client.detect_univariate_last_point(request)
        return response.is_anomaly
    except Exception as e:
        print(
            "Error code: {}".format(e.error.code),
            "Error message: {}".format(e.error.message),
        )
        return False


def write_anomaly_to_file(data):
    with open('anomalies.txt', 'a') as f:
        f.write(f"Anomaly detected: {data}\n")

series = []
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(msg.error())
            continue
                
        data = eval(msg.value().decode('utf-8'))
        series.append(TimeSeriesPoint(timestamp=data['timestamp'], value=data['value']))
        if len(series) < 288:
            continue

        if len(series) > 288:
            series.pop(0)

        print('analyzing event: ', data)
        if detect_anomalies(series):
            print(f"Anomaly detected: {data}")
            write_anomaly_to_file(data)
except:
    consumer.close()