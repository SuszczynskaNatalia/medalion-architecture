# Dokumentacja projektu – NYC Taxi Snowflake ETL

## 1. Cel projektu
Celem projektu jest zbudowanie architektury medalionowej dla danych NYC Taxi z wykorzystaniem Snowflake i Python. Źródłem danych jest oficjalny zbiór udostępniony przez New York City Taxi & Limousine Commission (TLC):
https://home4.nyc.gov/site/tlc/about/tlc-trip-record-data.page

---

## 2. Cel analityczny

Głównym celem analitycznym projektu jest odpowiedź na pytania:

* które **obszary Nowego Jorku generują największe przychody z kursów taxi**
* jaki wpływ na przychód mają **typ płatności, dostawca systemu (vendor) oraz taryfa**
* jak zmieniają się **wzorce przejazdów w czasie (miesiące, strefy, vendorzy)**

Aby umożliwić takie analizy powstały agregacje w warstwie Gold.

---

## 3. Warstwa Bronze

Warstwa Bronze przechowuje surowe dane dokładnie w takiej formie, w jakiej zostały pobrane ze źródła.

### Tabele

* `bronze.yellow_taxi_raw`
* `bronze.taxi_zones`

---

## 4. Warstwa Silver

Warstwa Silver zawiera oczyszczone i ustandaryzowane dane.

### Wykonane transformacje

* konwersja znaczników czasu (timestamp)
* usunięcie niepoprawnych kursów
* obsługa brakujących wartości
* utworzenie flag jakości danych
* normalizacja danych przy użyciu tabel słownikowych

### Tabele

* `silver.yellow_taxi_clean`
* `silver.taxi_zones_clean`
* `silver.vendor_lookup`
* `silver.payment_lookup`
* `silver.ratecode_lookup`
* `silver.calendar`

---

## 5. Warstwa Gold

Warstwa Gold zawiera zagregowane tabele analityczne, które mogą być bezpośrednio używane do raportowania lub analiz biznesowych.

### 5.1 Przychód według strefy i czasu

**Tabela:** `gold.yellow_taxi_revenue_by_zone_year_month`

Pozwala odpowiedzieć na pytania:

* które strefy odbioru i dowozu generują największy przychód
* jak zmienia się przychód w kolejnych miesiącach
* które trasy są najczęściej wykorzystywane

#### Metryki

* liczba kursów
* całkowity przychód
* suma napiwków
* średnia długość kursu
* liczba korekt finansowych

### 5.2 Przychód według vendora i typu płatności

**Tabela:** `gold.yellow_taxi_revenue_by_vendor_payment`

Pozwala analizować:

* który vendor generuje największy przychód
* jakie metody płatności są najczęściej używane
* które taryfy generują najwyższy przychód

#### Metryki

* liczba kursów
* całkowity przychód
* suma napiwków
* średnia długość kursu
* liczba korekt finansowych

---

## 6. Struktura folderów

```
project_root/
│
├─ scripts/               # Skrypty Python
│   └─ load_data.py       # Pobieranie i upload danych do Snowflake
│
├─ sql/                   # Pliki SQL do budowy medalionowej architektury
│   ├─ 01_create_database_and_schema.sql
│   ├─ 02_bronze_load_raw.sql
│   ├─ 03_silver_transformations.sql
│   └─ 04_gold_aggregations.sql
│
├─ data/                  # Katalog lokalny do pobieranych plików
│   └─ *.parquet / taxi_zone_lookup.csv
│
├─ docs/                  # Opisane problemy z danymi
│   └─ data_issues.md
│
└─ .env                   # Zmienne środowiskowe Snowflake
```

---

## 7. Skrypty i procesy

### 7.1 `scripts/ingest_to_snowflake.py`

* **Cel**: Pobiera dane NYC Taxi i Taxi Zone oraz uploaduje je do Snowflake stage.
* **Funkcjonalności**:

  * Wczytuje zmienne środowiskowe z `.env`.
  * Tworzy bazę danych i schemat w Snowflake, jeśli nie istnieją.
  * Tworzy stage’y:

    * `TAXI_ZONE_STAGE` – CSV Taxi Zone
    * `NYC_TAXI_INTERNAL_STAGE` – pliki Parquet NYC Taxi
  * Pobiera Taxi Zone CSV tylko raz i uploaduje jeśli nie istnieje na stage.
  * Pobiera pliki NYC Taxi w pętli po latach i miesiącach (YEARS, MONTHS), pomijając pliki już obecne na stage.
  * Obsługuje brak plików na zewnętrznym URL (`404`) bez przerywania procesu.
  * Pliki lokalne przechowywane są w folderze `data`.

**Przykład użycia**:

```bash
python scripts/load_data.py
```

### 7.2 Pliki SQL (`sql/`)

* **Cel**: Tworzenie medalionowej architektury w Snowflake.
* **Pliki**:
  * `01_create_database_and_schema.sql`
  * `02_bronze_load_raw.sql` – ładowanie danych surowych z stage do tabel Bronze.
  * `03_silver_transformations.sql` – transformacje, oczyszczanie danych, walidacja.
  * `04_gold_aggregations.sql` – tabele analityczne, agregacje i raporty.

### 7.3 Ładowanie danych do Snowflake

**Dane NYC Taxi:**

```sql
COPY INTO bronze.yellow_taxi_raw
FROM @BRONZE.NYC_TAXI_INTERNAL_STAGE
FILE_FORMAT = (TYPE = PARQUET);
```

**Dane o strefach taxi:**

```sql
COPY INTO bronze.taxi_zones
FROM @bronze.TAXI_ZONE_STAGE
FILE_FORMAT = (TYPE=CSV, FIELD_OPTIONALLY_ENCLOSED_BY='"', SKIP_HEADER=1);
```

Pliki są najpierw przesyłane do Snowflake Stage, a następnie ładowane do tabel.

---

## 8. Uruchamianie procesu

8.1 Skonfiguruj plik .env z danymi do Snowflake.
8.2 Uruchom skrypt Python, który pobiera dane i uploaduje je na stage: `python scripts/load_data.py`
8.3 Po zakończeniu pobierania i uploadu danych uruchom kolejno pliki SQL w Snowflake w następującej kolejności:
     01_create_database_and_schema.sql
     02_bronze_load_raw.sql
     03_silver_transformations.sql
     04_gold_aggregations.sql

```
