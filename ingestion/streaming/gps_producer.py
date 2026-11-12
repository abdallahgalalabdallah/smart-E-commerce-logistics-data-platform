import os
import sys
import time
import json
import uuid
import random
import logging
from datetime import datetime, timezone
from kafka import KafkaProducer
try:
    from kafka.errors import NoBrokersAvailable
except ImportError:
    from kafka.errors import KafkaError as NoBrokersAvailable

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("GPSProducer")

# إعدادات الاتصال بموزع كافكا
KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_NAME = os.getenv("KAFKA_TOPIC_TRACKING", "delivery_tracking")

# بيانات المحاكاة المرجعية (متوافقة مع جغرافية Olist في ساو باولو)
HUBS = ["HUB-SP-01", "HUB-SP-02", "HUB-RJ-01"]
ROUTES = ["ROUTE-101", "ROUTE-102", "ROUTE-103", "ROUTE-201"]
DRIVERS = [f"DRV-{i:03d}" for i in range(1, 11)]
VEHICLES = [f"VEH-{i:03d}" for i in range(1, 11)]
STATUSES = ["moving", "moving", "moving", "idle", "delayed"]

def get_kafka_producer():
    """محاولة الاتصال بـ Kafka مع إعادة المحاولة في حال تأخر بدء تشغيل الحاوية."""
    for attempt in range(1, 6):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks=1
            )
            logger.info("Connected to Kafka Broker successfully.")
            return producer
        except NoBrokersAvailable:
            logger.warning(f"Kafka broker not ready yet, retrying ({attempt}/5)...")
            time.sleep(3)
    logger.error("Could not connect to Kafka Broker. Aborting.")
    sys.exit(1)

def generate_telemetry():
    """توليد رسالة تتبع لحظية مع إدراج 5% بيانات خاطئة لاختبار الـ Quarantine."""
    lat_base = -23.5505
    lon_base = -46.6333
    idx = random.randint(0, len(DRIVERS) - 1)
    
    # نسبة 5% أخطاء لفحص نظام العزل في Spark و SQL Server
    is_anomaly = random.random() < 0.05

    payload = {
        "event_id": str(uuid.uuid4()),
        "event_version": 1,
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
        "driver_id": DRIVERS[idx],
        "vehicle_id": VEHICLES[idx],
        "route_id": random.choice(ROUTES),
        "hub_id": random.choice(HUBS),
        # في حالة الخطأ: إحداثيات مستحيلة (> 90) أو سرعة سالبة
        "latitude": 999.0 if is_anomaly else round(lat_base + random.uniform(-0.15, 0.15), 6),
        "longitude": round(lon_base + random.uniform(-0.15, 0.15), 6),
        "speed_kmh": -10.0 if is_anomaly else round(random.uniform(0.0, 95.0), 2),
        "status": random.choice(STATUSES)
    }
    return payload

def main():
    producer = get_kafka_producer()
    logger.info(f"Producing telemetry stream to topic: '{TOPIC_NAME}'...")

    try:
        while True:
            event = generate_telemetry()
            producer.send(TOPIC_NAME, value=event)
            logger.info(
                f"Sent -> Vehicle: {event['vehicle_id']} | Speed: {event['speed_kmh']} km/h | Status: {event['status']}"
            )
            time.sleep(1)  # إرسال رسالة كل ثانية
    except KeyboardInterrupt:
        logger.info("Stopping producer...")
    finally:
        producer.flush()
        producer.close()

if __name__ == "__main__":
    main()