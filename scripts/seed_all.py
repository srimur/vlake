# -*- coding: utf-8 -*-
"""
V-Lake: Seed all real data sources.
Windows/PowerShell safe - no unicode symbols, no os.system calls.

Run AFTER docker-compose up and AFTER deploy_contract.py.
Usage: python scripts/seed_all.py
"""
import json
import time
import csv
import io
import os
import sys
import random

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET = os.getenv("MINIO_SECRET_KEY", "minioadmin")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9093")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
PG_HOST = os.getenv("POSTGRES_HOST", "localhost")

def log(msg):
    # Windows-safe print — avoid charmap errors
    try:
        print(msg)
    except UnicodeEncodeError:
        print(msg.encode("ascii", "replace").decode())

def seed_minio():
    log("[MinIO] Seeding S3 bucket...")
    try:
        from minio import Minio
    except ImportError:
        log("  INSTALL: pip install minio")
        sys.exit(1)

    client = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS, secret_key=MINIO_SECRET, secure=False)
    bucket = "vlake-trial"
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
        log("  Created bucket: " + bucket)

    enrollment = [
        {"patient_id":"P0001","participant_name":"John Doe","age":52,"gender":"M","contact_phone":"555-0101","contact_email":"j.doe@trial.org","blood_type":"A+","site":"City Hospital","arm":"Treatment","enrollment_date":"2025-01-15","consent_status":"Active","investigator":"Dr. Williams"},
        {"patient_id":"P0002","participant_name":"Jane Smith","age":38,"gender":"F","contact_phone":"555-0102","contact_email":"j.smith@trial.org","blood_type":"O-","site":"City Hospital","arm":"Treatment","enrollment_date":"2025-01-20","consent_status":"Active","investigator":"Dr. Williams"},
        {"patient_id":"P0003","participant_name":"Robert Wilson","age":65,"gender":"M","contact_phone":"555-0103","contact_email":"r.wilson@trial.org","blood_type":"B+","site":"City Hospital","arm":"Placebo","enrollment_date":"2025-02-01","consent_status":"Active","investigator":"Dr. Williams"},
        {"patient_id":"P0004","participant_name":"Alice Brown","age":29,"gender":"F","contact_phone":"555-0104","contact_email":"a.brown@trial.org","blood_type":"AB+","site":"EU Satellite","arm":"Treatment","enrollment_date":"2025-02-10","consent_status":"Active","investigator":"Dr. Garcia"},
        {"patient_id":"P0005","participant_name":"Charlie Davis","age":57,"gender":"M","contact_phone":"555-0105","contact_email":"c.davis@trial.org","blood_type":"A-","site":"City Hospital","arm":"Placebo","enrollment_date":"2025-02-15","consent_status":"Withdrawn","investigator":"Dr. Williams"},
        {"patient_id":"P0006","participant_name":"Diana Miller","age":44,"gender":"F","contact_phone":"555-0106","contact_email":"d.miller@trial.org","blood_type":"O+","site":"EU Satellite","arm":"Treatment","enrollment_date":"2025-03-01","consent_status":"Active","investigator":"Dr. Garcia"},
        {"patient_id":"P0007","participant_name":"Edward Chen","age":71,"gender":"M","contact_phone":"555-0107","contact_email":"e.chen@trial.org","blood_type":"B-","site":"City Hospital","arm":"Treatment","enrollment_date":"2025-03-10","consent_status":"Active","investigator":"Dr. Williams"},
        {"patient_id":"P0008","participant_name":"Fiona Taylor","age":33,"gender":"F","contact_phone":"555-0108","contact_email":"f.taylor@trial.org","blood_type":"AB-","site":"EU Satellite","arm":"Placebo","enrollment_date":"2025-03-15","consent_status":"Active","investigator":"Dr. Garcia"},
    ]
    buf = io.BytesIO()
    wrapper = io.TextIOWrapper(buf, encoding="utf-8", write_through=True)
    w = csv.DictWriter(wrapper, fieldnames=enrollment[0].keys())
    w.writeheader()
    w.writerows(enrollment)
    wrapper.detach()
    buf.seek(0)
    client.put_object(bucket, "enrollment/trial_enrollment.csv", buf, length=buf.getbuffer().nbytes, content_type="text/csv")
    log("  Uploaded enrollment CSV (" + str(len(enrollment)) + " rows)")

    consent = b"INFORMED CONSENT FORM\nClinical Trial VLK-2025-Phase2\nParticipant John Doe P0001\nI voluntarily agree to participate.\nDate 2025-01-15 Witness Dr Williams\n"
    client.put_object(bucket, "documents/consent_P0001.txt", io.BytesIO(consent), length=len(consent), content_type="text/plain")
    log("  Uploaded consent document")
    return True

def seed_kafka():
    log("[Kafka] Producing adverse events...")
    try:
        from kafka import KafkaProducer
    except ImportError:
        log("  INSTALL: pip install kafka-python-ng")
        sys.exit(1)

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=5,
        request_timeout_ms=10000
    )
    events = [
        {"ae_id":"AE001","patient_id":"P0001","event":"Headache","severity":"Mild","causality":"Possible","onset_date":"2025-02-01","resolution_date":"2025-02-03","serious":False,"outcome":"Resolved","reported_by":"Dr. Williams"},
        {"ae_id":"AE002","patient_id":"P0002","event":"Nausea","severity":"Moderate","causality":"Probable","onset_date":"2025-02-15","resolution_date":"2025-02-18","serious":False,"outcome":"Resolved","reported_by":"Dr. Williams"},
        {"ae_id":"AE003","patient_id":"P0003","event":"Fatigue","severity":"Mild","causality":"Unlikely","onset_date":"2025-03-01","resolution_date":"","serious":False,"outcome":"Ongoing","reported_by":"Dr. Williams"},
        {"ae_id":"AE004","patient_id":"P0007","event":"Elevated ALT","severity":"Severe","causality":"Probable","onset_date":"2025-03-20","resolution_date":"","serious":True,"outcome":"Ongoing","reported_by":"Dr. Williams"},
        {"ae_id":"AE005","patient_id":"P0001","event":"Dizziness","severity":"Mild","causality":"Possible","onset_date":"2025-04-01","resolution_date":"2025-04-02","serious":False,"outcome":"Resolved","reported_by":"Dr. Williams"},
        {"ae_id":"AE006","patient_id":"P0004","event":"Rash","severity":"Moderate","causality":"Probable","onset_date":"2025-03-25","resolution_date":"2025-04-05","serious":False,"outcome":"Resolved","reported_by":"Dr. Garcia"},
    ]
    for ev in events:
        producer.send("adverse_events", value=ev)
    producer.flush()
    producer.close()
    log("  Produced " + str(len(events)) + " events to topic 'adverse_events'")
    return True

def seed_mongodb():
    log("[MongoDB] Inserting vitals...")
    try:
        from pymongo import MongoClient
    except ImportError:
        log("  INSTALL: pip install pymongo")
        sys.exit(1)

    random.seed(42)
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    db = client["vlake"]
    db.vitals_stream.drop()
    vitals = []
    for i in range(30):
        pid = "P000" + str((i % 4) + 1)
        ts = "2025-03-%02dT%02d:%02d:00Z" % (15 + i // 6, 8 + i % 12, (i * 17) % 60)
        vitals.append({
            "reading_id": "V%04d" % (i + 1), "patient_id": pid, "timestamp": ts,
            "heart_rate": random.randint(58, 105), "systolic_bp": random.randint(100, 160),
            "diastolic_bp": random.randint(60, 95), "spo2": round(random.uniform(94, 100), 1),
            "temperature_c": round(random.uniform(36.2, 38.8), 1),
            "respiratory_rate": random.randint(14, 24),
            "device_id": "IOT-%03d" % random.randint(1, 5),
            "alert": random.random() < 0.1,
        })
    db.vitals_stream.insert_many(vitals)
    client.close()
    log("  Inserted " + str(len(vitals)) + " vitals documents")
    return True

def check_postgres():
    log("[PostgreSQL] Verifying lab_results...")
    try:
        import psycopg2
    except ImportError:
        log("  INSTALL: pip install psycopg2-binary")
        sys.exit(1)

    conn = psycopg2.connect(host=PG_HOST, port=5432, dbname="vlake", user="vlake", password="vlake_secret")
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM lab_results")
    count = cur.fetchone()[0]
    conn.close()
    log("  lab_results has " + str(count) + " rows")
    return True

if __name__ == "__main__":
    log("=" * 60)
    log("  V-LAKE: Seeding All Data Sources")
    log("=" * 60)
    results = {}
    for name, fn in [("PostgreSQL", check_postgres), ("MinIO", seed_minio), ("Kafka", seed_kafka), ("MongoDB", seed_mongodb)]:
        try:
            results[name] = fn()
            log("  [OK] " + name)
        except Exception as e:
            log("  [FAIL] " + name + ": " + str(e))
            results[name] = False
    log("=" * 60)
    ok = all(results.values())
    for name, v in results.items():
        log("  " + ("[OK]  " if v else "[FAIL]") + " " + name)
    if not ok:
        log("\nSome services failed. Make sure docker-compose is running.")
        sys.exit(1)
    log("\nAll services seeded successfully.")
