import os
import requests
import snowflake.connector
from dotenv import load_dotenv
from pathlib import Path

env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(env_path)

# ======================================
# KONFIGURACJA
# ======================================

SNOWFLAKE_CONFIG = {
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "role" : "ACCOUNTADMIN",
    "warehouse" : os.getenv("SNOWFLAKE_WAREHOUSE")
}

DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
TAXI_ZONE_STAGE = "TAXI_ZONE_STAGE"
NYC_TAXI_STAGE = "NYC_TAXI_INTERNAL_STAGE"
DOWNLOAD_DIR = Path(__file__).resolve().parent.parent / "data"

TAXI_ZONE_CSV_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

YEARS = [2023, 2024]
MONTHS = range(1, 13)
TAXI_COLOR = "yellow"


def main() -> None:
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    # ======================================
    # Połączenie do Snowflake
    # ======================================
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cur = conn.cursor()

    cur.execute(f"USE DATABASE {DATABASE}")
    cur.execute(f"USE SCHEMA {SCHEMA}")

    # ======================================
    # Sprawdzenie i tworzenie stage
    # ======================================
    for stage_name, file_format in [(TAXI_ZONE_STAGE, "CSV FIELD_OPTIONALLY_ENCLOSED_BY='\"' SKIP_HEADER=1"),
                                    (NYC_TAXI_STAGE, "PARQUET")]:
        cur.execute(f"SHOW STAGES LIKE '{stage_name}' IN SCHEMA {DATABASE}.{SCHEMA}")
        if not cur.fetchone():
            cur.execute(f"""
            CREATE STAGE {stage_name}
            FILE_FORMAT = ({file_format})
            """)
            print(f"Stage {stage_name} utworzony")

    # ======================================
    # Pobranie i upload Taxi Zone CSV (raz)
    # ======================================
    zone_filename = TAXI_ZONE_CSV_URL.split("/")[-1]
    zone_filepath = os.path.join(DOWNLOAD_DIR, zone_filename)

    cur.execute(f"LIST @{TAXI_ZONE_STAGE}")
    existing_zone_files = [row[0].split('/')[-1] for row in cur.fetchall()]

    if zone_filename in existing_zone_files:
        print(f"Plik {zone_filename} JEST JUŻ na stage {TAXI_ZONE_STAGE}, pomijam pobieranie.")
    else:
        print(f"Pobieranie {zone_filename}...")
        r = requests.get(TAXI_ZONE_CSV_URL)
        r.raise_for_status()
        with open(zone_filepath, "wb") as f:
            f.write(r.content)
        print(f"{zone_filename} pobrany do {DOWNLOAD_DIR}")

        print(f"Upload {zone_filename} do stage {TAXI_ZONE_STAGE}...")
        cur.execute(f"""
        PUT file://{os.path.abspath(zone_filepath)}
        @{TAXI_ZONE_STAGE}
        AUTO_COMPRESS=FALSE
        OVERWRITE=TRUE
        """)
        print(f"Upload zakończony! Zwalniam miejsce usuwając pobrany plik lokalnie...")
        os.remove(zone_filepath)

    # ======================================
    # Pobieranie i upload NYC Taxi plików w pętli
    # ======================================
    cur.execute(f"LIST @{NYC_TAXI_STAGE}")
    existing_taxi_files = [row[0].split('/')[-1] for row in cur.fetchall()]

    for year in YEARS:
        for month in MONTHS:
            month_str = str(month).zfill(2)
            url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{TAXI_COLOR}_tripdata_{year}-{month_str}.parquet"
            filename = url.split("/")[-1]
            filepath = os.path.join(DOWNLOAD_DIR, filename)

            if filename in existing_taxi_files:
                print(f"-> Plik {filename} JEST JUŻ na stage {NYC_TAXI_STAGE}, pomijam pobieranie.")
                continue

            print(f"Pobieranie {filename}...")
            r = requests.get(url)
            if r.status_code != 200:
                print(f"Plik {filename} nie istnieje pod tym adresem URL, pomijam.")
                continue

            with open(filepath, "wb") as f:
                f.write(r.content)

            print(f"Upload {filename} do stage {NYC_TAXI_STAGE}...")
            cur.execute(f"""
            PUT file://{os.path.abspath(filepath)}
            @{NYC_TAXI_STAGE}
            AUTO_COMPRESS=FALSE
            OVERWRITE=TRUE
            """)
            print(f"Upload {filename} zakończony. Zwalniam miejsce na dysku...")
            os.remove(filepath)

    print("Wszystkie pliki załadowane pomyślnie i dysk lokalny wyczyszczony.")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()