import os
import json
import time
import uuid
import random
import logging
from datetime import datetime, timezone
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv(override=True)

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("gps-producer")

BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
##BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "delivery_tracking")
INTERVAL_SEC = float(os.getenv("PRODUCER_INTERVAL_SEC", "1.0"))
BAD_EVENT_RATE = float(os.getenv("PRODUCER_BAD_EVENT_RATE", "0.08"))  # 8% رسائل مرفوضة للـ Quarantine

# تعريف المراكز (Hubs) مع إحداثياتها الحقيقية في البرازيل لتتطابق مع بيانات Olist
HUBS = {
    "HUB-SP": {"lat": -23.5505, "lon": -46.6333, "routes": ["ROUTE-SP-01", "ROUTE-SP-02"]},
    "HUB-RJ": {"lat": -22.9068, "lon": -43.1729, "routes": ["ROUTE-RJ-01", "ROUTE-RJ-02"]},
    "HUB-MG": {"lat": -19.9167, "lon": -43.9345, "routes": ["ROUTE-MG-01"]},
    "HUB-PR": {"lat": -25.4284, "lon": -49.2733, "routes": ["ROUTE-PR-01"]},
}

# أوزان النشاط لجعل بعض الـ Hubs والـ Routes أكثر ازدحاماً ونشاطاً من غيرها (لـ Dashboard 1)
VEHICLE_PROFILES = [
    # HUB-SP (الأعلى نشاطاً)
    ("VEH-101", "DRV-01", "HUB-SP", "ROUTE-SP-01"),
    ("VEH-102", "DRV-02", "HUB-SP", "ROUTE-SP-01"),
    ("VEH-103", "DRV-03", "HUB-SP", "ROUTE-SP-01"),
    ("VEH-104", "DRV-04", "HUB-SP", "ROUTE-SP-02"),
    ("VEH-105", "DRV-05", "HUB-SP", "ROUTE-SP-02"),
    ("VEH-106", "DRV-06", "HUB-SP", "ROUTE-SP-02"),
    # HUB-RJ (نشاط متوسط-مرتفع)
    ("VEH-201", "DRV-07", "HUB-RJ", "ROUTE-RJ-01"),
    ("VEH-202", "DRV-08", "HUB-RJ", "ROUTE-RJ-01"),
    ("VEH-203", "DRV-09", "HUB-RJ", "ROUTE-RJ-02"),
    ("VEH-204", "DRV-10", "HUB-RJ", "ROUTE-RJ-02"),
    # HUB-MG (نشاط متوسط)
    ("VEH-301", "DRV-11", "HUB-MG", "ROUTE-MG-01"),
    ("VEH-302", "DRV-12", "HUB-MG", "ROUTE-MG-01"),
    ("VEH-303", "DRV-13", "HUB-MG", "ROUTE-MG-01"),
    # HUB-PR (نشاط أقل)
    ("VEH-401", "DRV-14", "HUB-PR", "ROUTE-PR-01"),
    ("VEH-402", "DRV-15", "HUB-PR", "ROUTE-PR-01"),
]

# تهيئة الحالة الحية لكل مركبة
fleet_state = {}
for veh_id, drv_id, hub_id, route_id in VEHICLE_PROFILES:
    base = HUBS[hub_id]
    fleet_state[veh_id] = {
        "driver_id": drv_id,
        "vehicle_id": veh_id,
        "hub_id": hub_id,
        "route_id": route_id,
        "latitude": base["lat"] + random.uniform(-0.08, 0.08),
        "longitude": base["lon"] + random.uniform(-0.08, 0.08),
        "speed_kmh": round(random.uniform(35.0, 85.0), 1),
        "status": "moving",
    }

def step_vehicle(v: dict) -> dict:
    """تحديث حركة المركبة وسرعتها وحالتها بشكل واقعي."""
    # احتمالات تغيير الحالة: 65% moving، 20% idle، 15% delayed
    roll = random.random()
    if roll < 0.65:
        v["status"] = "moving"
        v["speed_kmh"] = round(max(20.0, min(115.0, v["speed_kmh"] + random.uniform(-10.0, 12.0))), 1)
        v["latitude"] = round(v["latitude"] + random.uniform(-0.004, 0.004), 6)
        v["longitude"] = round(v["longitude"] + random.uniform(-0.004, 0.004), 6)
    elif roll < 0.85:
        v["status"] = "idle"
        v["speed_kmh"] = 0.0
    else:
        v["status"] = "delayed"
        v["speed_kmh"] = round(random.uniform(3.0, 18.0), 1)
        v["latitude"] = round(v["latitude"] + random.uniform(-0.001, 0.001), 6)
        v["longitude"] = round(v["longitude"] + random.uniform(-0.001, 0.001), 6)

    return {
        "event_id": str(uuid.uuid4()),
        "event_version": 1,
        "event_timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
        "driver_id": v["driver_id"],
        "vehicle_id": v["vehicle_id"],
        "route_id": v["route_id"],
        "hub_id": v["hub_id"],
        "latitude": v["latitude"],
        "longitude": v["longitude"],
        "speed_kmh": v["speed_kmh"],
        "status": v["status"],
    }

def generate_bad_event() -> str:
    """توليد حدث غير صالح لاختبار جدول Quarantine_Tracking ومؤشر الأحداث المرفوضة."""
    bad_type = random.choice(["invalid_speed", "invalid_coords", "bad_status", "missing_driver", "malformed_json"])
    if bad_type == "malformed_json":
        return '{"event_id": "broken-json", "speed_kmh": '

    payload = {
        "event_id": str(uuid.uuid4()),
        "event_version": 1,
        "event_timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
        "driver_id": "DRV-99",
        "vehicle_id": "VEH-999",
        "route_id": "ROUTE-SP-01",
        "hub_id": "HUB-SP",
        "latitude": -23.55,
        "longitude": -46.63,
        "speed_kmh": 55.0,
        "status": "moving",
    }
    if bad_type == "invalid_speed":
        payload["speed_kmh"] = 240.0  # يتجاوز الحد الأقصى 160 كم/س
    elif bad_type == "invalid_coords":
        payload["latitude"] = 145.0   # خارج نطاق [-90, 90]
    elif bad_type == "bad_status":
        payload["status"] = "unknown_state"
    elif bad_type == "missing_driver":
        payload["driver_id"] = None

    return json.dumps(payload)

def main():
    producer = KafkaProducer(
        bootstrap_servers=BROKER,
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        value_serializer=lambda v: v.encode("utf-8"),
        acks="all",
    )
    log.info("GPS Producer started -> broker=%s topic=%s fleet_size=%d", BROKER, TOPIC, len(fleet_state))

    batch_num = 0
    try:
        while True:
            batch_num += 1
            sent_valid = 0
            sent_bad = 0

            # إرسال تحديث لكل مركبة في الأسطول
            for veh_id, state in fleet_state.items():
                event = step_vehicle(state)
                producer.send(TOPIC, key=veh_id, value=json.dumps(event))
                sent_valid += 1

            # إرسال 1-2 حدث معيب بشكل دوري لتغذية Quarantine_Tracking
            if random.random() < (BAD_EVENT_RATE * 10):
                bad_raw = generate_bad_event()
                producer.send(TOPIC, key="BAD-EVENT", value=bad_raw)
                sent_bad += 1

            producer.flush()
            log.info("Cycle #%d | sent_valid=%d | sent_bad=%d", batch_num, sent_valid, sent_bad)
            time.sleep(INTERVAL_SEC)
    except KeyboardInterrupt:
        log.info("Stopping GPS Producer...")
    finally:
        producer.close()

if __name__ == "__main__":
    main()