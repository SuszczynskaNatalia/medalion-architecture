1. Niezgodność dat z plikiem źródłowym (date_mismatch_flag)

Opis problemu:
Niektóre rekordy mają pickup_date lub dropoff_date poza zakresem dat wskazanym przez nazwę pliku Parquet (np. plik yellow_tripdata_2023-03.parquet zawiera kursy z 2023-04). W Bronze są wszystkie dane surowe, więc te rekordy istnieją, ale mogą wprowadzać błędy przy analizach miesięcznych lub dzielnicowych.

Konsekwencje biznesowe:
- Błędne sumy przychodów lub napiwków w analizach miesięcznych
- Trudności w raportach czasowych i trendach

Rozwiązanie w pipeline:
- W Silver dodajemy kolumnę date_mismatch_flag (TRUE/FALSE)


2. Ujemne wartości w opłatach (fare_amount < 0 i improvement_surcharge)

Opis problemu:
Niektóre kursy mają ujemną wartość fare_amount lub nietypowe improvement_surcharge (np. -0.3, 0.3, 1, -1). Mogą to być korekty, anulacje lub błędy systemowe.

Konsekwencje biznesowe: Niepoprawny całkowity przychód w analizach. Zafałszowane KPI przy porównaniach między dzielnicami, dniami tygodnia czy typami płatności

Rozwiązanie w pipeline:
- W Silver tworzymy flagę fare_correction lub improvement_correction

W Gold można agregować liczbę i sumę korekt (total_corrections). Pozwala to na analizę wpływu korekt bez usuwania danych.


3. Braki danych / nieznane kody (NULL lub kod 99 / 5 / 6 w lookupach)

Opis problemu: Kolumny takie jak RatecodeID = 99 (Null/unknown) lub payment_type = 5 (Unknown). VendorID lub PULocationID / DOLocationID czasem są NULL

Konsekwencje biznesowe:
- Trudności w grupowaniu i analizie według vendorów, taryf lub typów płatności
- Niewiarygodne raporty KPI, np. całkowity przychód według dzielnicy lub typu płatności

Rozwiązanie w pipeline:
- W Silver zachowujemy te wartości, ale mapujemy je w lookup tables na np. Unknown / Null

W Gold można je osobno analizować lub filtrować, aby nie psuły agregacji