"""
Narzędzie do ręcznego wysyłania wiadomości do Kafki.
Używane do testowania taxi_streaming_pipeline.

Przykłady użycia (z katalogu głównego projektu):

  # Wyślij jeden plik (domyślny miesiąc 2023-01)
  python scripts/kafka_producer.py

  # Wyślij konkretny plik
  python scripts/kafka_producer.py yellow_tripdata_2023-06.parquet

  # Wyślij kilka plików naraz
  python scripts/kafka_producer.py yellow_tripdata_2023-01.parquet yellow_tripdata_2023-02.parquet

Domyślny bootstrap: localhost:9094 (Kafka dostępna z hosta przez port 9094).
Nadpisz przez zmienną środowiskową: KAFKA_BOOTSTRAP_SERVERS=localhost:9094
"""
from __future__ import annotations

import json
import os
import sys
from datetime import date

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
TOPIC     = os.getenv("KAFKA_TOPIC", "nyc_taxi_events")


def make_filename(year: int, month: int, color: str = "yellow") -> str:
    return f"{color}_tripdata_{year}-{month:02d}.parquet"


def send(producer: KafkaProducer, filename: str) -> None:
    payload = json.dumps({"filename": filename}).encode("utf-8")
    future  = producer.send(TOPIC, value=payload)
    meta    = future.get(timeout=10)
    print(f"[OK] Wysłano: {filename} -> {TOPIC} (partition={meta.partition}, offset={meta.offset})")


def main() -> int:
    filenames: list[str] = sys.argv[1:]
    if not filenames:
        today    = date.today()
        filenames = [make_filename(today.year, today.month)]
        print(f"[INFO] Brak argumentów – wyślę bieżący miesiąc: {filenames[0]}")

    try:
        producer = KafkaProducer(bootstrap_servers=BOOTSTRAP)
    except NoBrokersAvailable:
        print(
            f"[ERROR] Nie można połączyć z Kafką pod {BOOTSTRAP}.\n"
            "Upewnij się, że docker compose up -d kafka jest uruchomione.",
            file=sys.stderr,
        )
        return 1

    for fname in filenames:
        send(producer, fname)

    producer.flush()
    producer.close()
    print(f"[DONE] Wysłano {len(filenames)} wiadomość(i) do topiku '{TOPIC}'.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
