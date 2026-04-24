"""
CLI wywoływany przez BashOperator w taxi_streaming_pipeline.

Polecenia:
  listen           – odczytuje 1 wiadomość z Kafki (pole 'filename' w JSON).
                     Wypisuje nazwę pliku na stdout (→ XCom), logi na stderr.
                     Przy KAFKA_LISTEN_NO_FAIL=1 brak wiadomości daje exit 0
                     z pustym wyjściem (graceful skip w automatycznym DAG-u).
  upload <plik>    – pobiera plik Parquet z TLC (HTTP) i uploaduje go
                     do Snowflake Stage NYC_TAXI_INTERNAL_STAGE (PUT).
                     Lokalny plik jest kasowany po udanym uploadzie.
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import requests
import snowflake.connector
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

# ── Snowflake (identyczna konfiguracja co ingest_to_snowflake.py) ────────────
SNOWFLAKE_CONFIG = {
    "user":      os.getenv("SNOWFLAKE_USER"),
    "password":  os.getenv("SNOWFLAKE_PASSWORD"),
    "account":   os.getenv("SNOWFLAKE_ACCOUNT"),
    "role":      "ACCOUNTADMIN",
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
}
DATABASE       = os.getenv("SNOWFLAKE_DATABASE")
SCHEMA         = os.getenv("SNOWFLAKE_SCHEMA")
NYC_TAXI_STAGE = "NYC_TAXI_INTERNAL_STAGE"

# ── Kafka ────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC      = os.getenv("KAFKA_TOPIC", "nyc_taxi_events")
KAFKA_GROUP      = os.getenv("KAFKA_CONSUMER_GROUP", "airflow_streaming_group")
KAFKA_TIMEOUT_MS = int(os.getenv("KAFKA_CONSUMER_TIMEOUT_MS", "15000"))
LISTEN_NO_FAIL   = os.getenv("KAFKA_LISTEN_NO_FAIL", "0").strip() == "1"

# ── Ścieżki ──────────────────────────────────────────────────────────────────
DOWNLOAD_DIR = Path(__file__).resolve().parent.parent / "data"
DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
TLC_BASE = "https://d37ci6vzurychx.cloudfront.net/trip-data"


def eprint(*args, **kwargs) -> None:
    print(*args, file=sys.stderr, **kwargs)


# ── listen ───────────────────────────────────────────────────────────────────
def cmd_listen() -> int:
    from kafka import KafkaConsumer

    eprint(f"[KAFKA] Łączę z {KAFKA_BOOTSTRAP}, topic={KAFKA_TOPIC}, group={KAFKA_GROUP}")
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=KAFKA_GROUP,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        consumer_timeout_ms=KAFKA_TIMEOUT_MS,
    )
    for message in consumer:
        try:
            payload  = json.loads(message.value.decode("utf-8"))
            filename = payload.get("filename", "").strip()
            if filename:
                eprint(f"[KAFKA] Odczytano: {filename}")
                print(filename, end="")   # tylko ta linia trafia do XCom
                return 0
        except Exception as exc:
            eprint(f"[ERROR] Niepoprawny JSON: {exc}")

    eprint("[KAFKA] Brak wiadomości w topiku (timeout).")
    if LISTEN_NO_FAIL:
        print("", end="")   # pusty XCom → ShortCircuitOperator przerwie dalsze taski
        return 0
    return 1


# ── upload ───────────────────────────────────────────────────────────────────
def cmd_upload(filename: str) -> int:
    local_path = DOWNLOAD_DIR / filename
    url = f"{TLC_BASE}/{filename}"

    # 1) Pobranie HTTP
    eprint(f"[HTTP] Pobieranie {url}")
    r = requests.get(url, timeout=300)
    if r.status_code == 404:
        eprint(f"[ERROR] Plik {filename} nie istnieje pod {url}")
        return 1
    r.raise_for_status()
    local_path.write_bytes(r.content)
    eprint(f"[OK]   Zapisano lokalnie: {local_path}")

    # 2) PUT do Snowflake Stage
    eprint(f"[SNOWFLAKE] PUT -> @{DATABASE}.{SCHEMA}.{NYC_TAXI_STAGE}")
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cur  = conn.cursor()
    try:
        cur.execute(f"USE DATABASE {DATABASE}")
        cur.execute(f"USE SCHEMA {SCHEMA}")
        cur.execute(f"""
            PUT file://{local_path.resolve()}
            @{NYC_TAXI_STAGE}
            AUTO_COMPRESS=FALSE
            OVERWRITE=TRUE
        """)
        eprint("[OK]   Upload do Stage zakończony.")
    finally:
        cur.close()
        conn.close()
        try:
            local_path.unlink()
            eprint(f"[CLEANUP] Usunięto lokalny plik: {local_path}")
        except OSError as exc:
            eprint(f"[CLEANUP] {exc}")
    return 0


# ── main ─────────────────────────────────────────────────────────────────────
def main() -> int:
    if len(sys.argv) < 2:
        eprint("Użycie: taxi_streaming_cli.py listen | upload <filename>")
        return 1
    cmd = sys.argv[1]
    if cmd == "listen":
        return cmd_listen()
    if cmd == "upload":
        if len(sys.argv) < 3:
            eprint("Użycie: taxi_streaming_cli.py upload <filename>")
            return 1
        return cmd_upload(sys.argv[2])
    eprint(f"[ERROR] Nieznane polecenie: {cmd!r}")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
