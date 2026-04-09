# Dokumentacja projektu – NYC Taxi: End-to-End Data Engineering Pipeline

## 1. Cel projektu
Celem projektu jest zbudowanie zautomatyzowanego potoku danych w architekturze medalionowej dla danych NYC Taxi. Źródłem danych jest oficjalny zbiór udostępniony przez New York City Taxi & Limousine Commission (TLC):
[TLC Trip Record Data](https://home4.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

Projekt ewoluował z manualnego uruchamiania skryptów SQL do w pełni zautomatyzowanego, wyizolowanego środowiska orkiestrowanego przez **Apache Airflow** i transformowanego za pomocą **dbt Core**.

---

## 2. Architektura i Tech Stack

* **Hurtownia danych:** Snowflake
* **Orkiestracja:** Apache Airflow (Docker, LocalExecutor, PostgreSQL backend)
* **Transformacje:** dbt Core (Data Build Tool)
* **Integracja Airflow & dbt:** Astronomer Cosmos (dynamiczne parsowanie modeli dbt na zadania Airflow)
* **Języki i skrypty:** Python (pobieranie danych API), SQL (transformacje)
* **Infrastruktura:** Docker & Docker Compose (z wyizolowanym środowiskiem wirtualnym `venv` dla dbt w celu uniknięcia konfliktów zależności)

---

## 3. Cel analityczny
Głównym celem analitycznym projektu jest odpowiedź na pytania:
* które **obszary Nowego Jorku generują największe przychody z kursów taxi**
* jaki wpływ na przychód mają **typ płatności, dostawca systemu (vendor) oraz taryfa**
* jak zmieniają się **wzorce przejazdów w czasie (miesiące, strefy, vendorzy)**

Aby umożliwić takie analizy, w warstwie Gold powstają zautomatyzowane agregacje.

---

## 4. Architektura Medalionowa (Warstwy Danych)

### Warstwa Bronze
Przechowuje surowe dane dokładnie w takiej formie, w jakiej zostały pobrane ze źródła do Snowflake Stage.
* `bronze.yellow_taxi_raw`
* `bronze.taxi_zones`

### Warstwa Silver (modele dbt)
Zawiera oczyszczone i ustandaryzowane dane. Proces oparty o modele dbt.
* **Procesy:** konwersja znaczników czasu (timestamp), usunięcie niepoprawnych kursów, obsługa brakujących wartości, normalizacja danych przy użyciu tabel słownikowych.
* **Testy Data Quality:** Wprowadzono rygorystyczne testy dbt (`not_null`, `unique`) oraz niestandardowe testy logiki biznesowej (np. weryfikacja czy składowe rachunku matematycznie sumują się do kolumny `total_amount`, zwracające `Ostrzeżenia (Warn)` o anomaliach bilingowych systemów w NYC).
* **Tabele:** `silver.yellow_taxi_clean`, `silver.taxi_zones_clean`, tabele słownikowe (`dim_vendor`, `dim_payment_type`, `dim_ratecode`), wymiar czasu (`dim_calendar`).

### Warstwa Gold (modele dbt)
Zawiera zagregowane tabele analityczne, bezpośrednio zasilające kokpity BI.
* **`gold.revenue_by_zone`**: Analiza przychodów wg stref odbioru/dowozu i czasu. Metryki: liczba kursów, całkowity przychód, napiwki, średnia długość.
* **`gold.revenue_by_vendor_payment`**: Analiza dystrybucji vendorów i typów płatności.

---

## 5. Struktura folderów

```text
taxi_project/
│
├─ dags/                           # Definicje DAG-ów Airflow i skrypty Ingestion
│   ├─ sql/
│   │   ├─ bronze/
│   │   │   └─ load_raw.sql        # Ładowanie danych do tabel warstwy Bronze
│   │   └─ setup/
│   │       └─ create_database_and_schema.sql # Setup bazy i schematów w Snowflake
│   ├─ ingest_to_snowflake.py      # Pobieranie plików Parquet i upload na Snowflake Stage
│   └─ taxi_pipeline.py            # Główny potok orkiestrowany przez potok Cosmos i dbt
│
├─ dbt/                            # Główny katalog projektu dbt
│   ├─ macros/
│   │   └─ generate_schema_name.sql # Niestandardowe makra (nadpisywanie nazw schematów)
│   ├─ models/
│   │   ├─ gold/                   # Modele transformacji (agregacje biznesowe)
│   │   │   ├─ gold_models.yml
│   │   │   ├─ revenue_by_vendor_payment.sql
│   │   │   └─ revenue_by_zone.sql
│   │   └─ silver/                 # Modele transformacji (oczyszczanie danych)
│   │       ├─ _silver_models.yml
│   │       ├─ _sources.yml
│   │       ├─ dim_calendar.sql
│   │       ├─ dim_payment_type.sql
│   │       ├─ dim_ratecode.sql
│   │       ├─ dim_taxi_zones.sql
│   │       ├─ dim_vendor.sql
│   │       └─ yellow_taxi.sql
│   ├─ tests/                      # Niestandardowe testy SQL Data Quality
│   │   ├─ assert_tips_not_exceed_revenue.sql
│   │   └─ assert_total_amount_math_silver.sql
│   ├─ dbt_project.yml             # Konfiguracja bazowa projektu dbt
│   └─ profiles.yml                # Ustawienia połączenia dbt <-> Snowflake
│
├─ data/                           # Katalog powiązany lokalnie - miejsce na pobierane pliki
├─ logs/                           # Zmapowane logi z kontenerów Airflow i dbt
├─ Dockerfile                      # Definicja niestandardowego obrazu Airflow (wyizolowane środowisko dbt_venv)
├─ docker-compose.yaml             # Infrastruktura Dockerowa
├─ requirements.txt                # Zależności Pythonowe, pakiety Cosmos i Airflow Providers
└─ .env                            # Zmienne środowiskowe Snowflake i Airflow (autoryzacja)