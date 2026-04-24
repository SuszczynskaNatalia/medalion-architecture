# NYC Taxi – End-to-End Data Engineering Pipeline

## 1. Cel projektu

Zautomatyzowany potok danych w architekturze medalionowej dla danych NYC Yellow Taxi.  
Źródło: [TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

Projekt obsługuje **dwa niezależne tryby zasilania**:
- **Batch** – historyczne i codzienne przyrostowe ładowanie plików Parquet z TLC
- **Streaming** – automatyczne wstrzykiwanie świeżych danych przez kolejkę **Apache Kafka**

---

## 2. Architektura i tech stack

| Warstwa / rola | Technologia |
|----------------|-------------|
| **Hurtownia danych** | Snowflake (Bronze / Silver / Gold) |
| **Metadane Airflow** | PostgreSQL |
| **Orkiestracja** | Apache Airflow 2.9 (LocalExecutor, Docker) |
| **Transformacje** | dbt Core + Astronomer Cosmos (Silver, Gold) |
| **Kolejkowanie** | Apache Kafka (KRaft, Confluent) + `kafka-python` |
| **Języki** | Python, SQL |
| **Infrastruktura** | Docker Compose (Airflow, Kafka, Postgres, Kafka UI) |

Środowisko dbt uruchamiane w wyizolowanym venv `/opt/airflow/dbt_venv` wewnątrz obrazu Airflow.

---

## 3. Architektura medalionowa (Snowflake)

### Bronze
Surowe dane załadowane przez `COPY INTO` ze Snowflake Stage.
- `bronze.yellow_taxi_raw` – pliki Parquet z TLC
- `bronze.taxi_zones` – słownik stref NYC (CSV)

### Silver (dbt)
Oczyszczanie i normalizacja:
- `yellow_taxi` – kursy po filtracji i rzutowaniu typów
- tabele słownikowe: `dim_vendor`, `dim_payment_type`, `dim_ratecode`, `dim_taxi_zones`, `dim_calendar`
- testy Data Quality (`not_null`, `unique`, własne testy logiki finansowej)

### Gold (dbt)
Agregacje analityczne zasilające BI:
- `revenue_by_zone` – przychody według stref i czasu
- `revenue_by_vendor_payment` – dystrybucja wg vendora i płatności

---

## 4. DAG-i Airflow

### Batch pipeline – `dags/taxi_pipeline.py`

| `dag_id` | Trigger | Opis |
|----------|---------|------|
| `nyc_taxi_end_to_end_pipeline` | r�czny (`schedule=None`) | Pełny ELT: Python → Snowflake Stage → `COPY INTO` Bronze → dbt Silver → dbt Gold |

Kroki (kolejność w DAG-u):
1. `setup_snowflake_environment` – `SQLExecuteQueryOperator`: DDL – tworzy bazę, schematy, tabele
2. `ingest_files_to_snowflake_stage` – `ingest_to_snowflake.py`: HTTP GET + `PUT` plików na Stage
3. `load_data_to_bronze` – `COPY INTO` z Stage do tabel Bronze
4. `transform_silver` – `DbtTaskGroup` (Cosmos)
5. `transform_gold` – `DbtTaskGroup` (Cosmos)

### Streaming pipeline – `dags/taxi_streaming_pipeline.py`

| `dag_id` | Trigger | Opis |
|----------|---------|------|
| `taxi_streaming_http` | ręczny (`schedule=None`) | Ad-hoc: 1 wiadomość Kafka → Stage → Bronze → dbt |
| `nyc_taxi_streaming_incremental` | `timedelta(days=1)` – automatyczny | Codziennie odpytuje Kafkę; brak wiadomości = graceful skip |

Kroki (oba DAG-i, po odebraniu `filename` z Kafki):
1. `listen_kafka` – `taxi_streaming_cli.py listen` → filename na stdout (XCom)
2. `check_message` *(tylko auto DAG)* – `ShortCircuitOperator`; pomija resztę jeśli brak wiadomości
3. `upload_to_stage` – `taxi_streaming_cli.py upload <filename>`: HTTP + `PUT` Snowflake Stage
4. `setup_snowflake_environment` – DDL Snowflake
5. `load_data_to_bronze` – `COPY INTO` Bronze
6. `transform_silver` / `transform_gold` – dbt via Cosmos

### Skrypt CLI – `dags/taxi_streaming_cli.py`

| Polecenie | Działanie |
|-----------|-----------|
| `listen` | Łączy z Kafką (`nyc_taxi_events`), odczytuje JSON `{"filename": "..."}`, zwraca nazwę pliku na stdout |
| `upload <plik>` | HTTP GET z TLC → `PUT` do `NYC_TAXI_INTERNAL_STAGE` w Snowflake → usuwa lokalny plik |

---

## 5. Infrastruktura Docker (`docker-compose.yml`)

| Serwis | Obraz | Port | Rola |
|--------|-------|------|------|
| `postgres` | postgres:15 | — | Metadane Airflow |
| `airflow-init` | *(build)* | — | Migracja DB + konto admin |
| `airflow-webserver` | *(build)* | **8080** | UI Airflow |
| `airflow-scheduler` | *(build)* | — | Uruchamianie DAG-ów |
| `kafka` | cp-kafka:7.6.1 | **9094** *(host)* | Broker Kafka KRaft (bez Zookeeper) |
| `kafka-init` | cp-kafka:7.6.1 | — | Jednorazowe tworzenie topiku `nyc_taxi_events` |
| `kafka-ui` | kafka-ui:latest | **8090** | Podgląd topików i wiadomości |

Kontenery Airflow dostają automatycznie:
- `KAFKA_BOOTSTRAP_SERVERS=kafka:9092`
- `KAFKA_TOPIC=nyc_taxi_events`
- wszystkie zmienne Snowflake z `.env`

---

## 6. Uruchomienie

### Wymagania wstępne
- Docker Desktop
- Plik `.env` z uzupełnionymi wartościami (wzorzec: `.env.example`)

### Start całego stacku

```bash
docker compose up -d
```

Poczekaj ~60 s na pełne uruchomienie brokera Kafka i migrację bazy Airflow.

| UI | Adres | Login |
|----|-------|-------|
| Airflow | http://localhost:8080 | admin / admin |
| Kafka UI | http://localhost:8090 | — |

### Wysłanie wiadomości do Kafki (lokalnie z hosta)

```bash
# Zainstaluj kafka-python lokalnie (jednorazowo)
pip install kafka-python

# Wyślij jeden plik (bieżący miesiąc)
python scripts/kafka_producer.py

# Wyślij konkretny miesiąc
python scripts/kafka_producer.py yellow_tripdata_2023-06.parquet

# Kilka plików naraz
python scripts/kafka_producer.py yellow_tripdata_2023-01.parquet yellow_tripdata_2023-02.parquet
```

Domyślny bootstrap hosta: `localhost:9094`. Kafka w kontenerach Airflow używa `kafka:9092`.

### Trigger DAG-a po wysłaniu wiadomości

**Ad-hoc (ręczny):**
1. Wyślij wiadomość przez `kafka_producer.py`
2. W UI Airflow → `taxi_streaming_http` → **Trigger DAG**

**Automatyczny:**
- `nyc_taxi_streaming_incremental` uruchamia się codziennie o północy
- jeśli w topiku jest nowa wiadomość → pełny pipeline
- jeśli brak → zielony skip (bez błędu)

---

## 7. Zmienne środowiskowe (`.env`)

Wzorzec: `.env.example`

| Zmienna | Opis |
|---------|------|
| `SNOWFLAKE_USER` | Użytkownik Snowflake |
| `SNOWFLAKE_PASSWORD` | Hasło |
| `SNOWFLAKE_ACCOUNT` | Identyfikator konta (np. `xy12345.eu-west-1`) |
| `SNOWFLAKE_DATABASE` | Baza danych (np. `TAXI_PROJECT`) |
| `SNOWFLAKE_SCHEMA` | Schemat domyślny (np. `BRONZE`) |
| `SNOWFLAKE_WAREHOUSE` | Warehouse (np. `COMPUTE_WH`) |
| `SNOWFLAKE_ROLE` | Rola (np. `ACCOUNTADMIN`) |
| `AIRFLOW_SECRET_KEY` | Losowy klucz serwera WWW Airflow (min. 32 znaki) |

---

## 8. Struktura katalogów

```text
taxi_project/
├── dags/
│   ├── sql/
│   │   ├── bronze/load_raw.sql              # COPY INTO Bronze (Snowflake)
│   │   └── setup/create_database_and_schema.sql
│   ├── ingest_to_snowflake.py               # HTTP → Snowflake Stage (batch)
│   ├── taxi_pipeline.py                     # DAG batch (trigger r�czny)
│   ├── taxi_streaming_cli.py                # CLI: Kafka listen + Snowflake upload
│   └── taxi_streaming_pipeline.py           # DAG streaming (ad-hoc + automatyczny)
├── dbt/
│   ├── models/
│   │   ├── silver/                          # Oczyszczanie i normalizacja
│   │   └── gold/                            # Agregacje analityczne
│   ├── tests/                               # Testy Data Quality
│   ├── dbt_project.yml
│   └── profiles.yml                         # Połączenie dbt → Snowflake
├── scripts/
│   └── kafka_producer.py                    # Wysyłanie wiadomości do Kafki (testowanie)
├── docs/
│   ├── architecture.png
│   └── erd_silver.png
├── logs/
├── Dockerfile                               # Obraz Airflow + dbt_venv
├── docker-compose.yml                       # Cały stack (Airflow, Kafka, Postgres)
├── requirements.txt
├── .env.example
└── .env
```
