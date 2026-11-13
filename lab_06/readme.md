# Przetwarzanie dużych zbiorów danych

## Lab 6: PySpark DataFrames – przetwarzanie danych ustrukturyzowanych

W tym laboratorium przechodzimy z niskopoziomowego interfejsu RDD (poznanego na Lab 5) do wysokopoziomowego API **Spark DataFrame (`pyspark.sql`)** w **Apache Spark 4.x**.

Poznamy praktyczny potok przetwarzania i analizy danych tabelarycznych:
1. **Wczytywanie danych rzeczywistych i pułapki formatu CSV** (*UCI Recipe Reviews and User Feedback Dataset*) – obsługa rekordów wieloliniowych (`multiLine`), znaków ucieczki (`escape`), automatyczne wnioskowanie typów (`inferSchema`) vs jawna definicja schematu (`StructType`).
2. **Wyrażenia kolumnowe (`Column API`) i transformacje** – projekcja (`select`), tworzenie i modyfikacja kolumn (`withColumn`, `withColumnRenamed`), wyrażenia warunkowe (`when` / `otherwise`), potoki `transform()`.
3. **Diagnostyka jakości danych i filtrowanie** – wykrywanie wartości `NULL` oraz `NaN`, czyszczenie ramek (`na.drop`, `na.fill`), filtrowanie wielowarunkowe (`filter` / `where`).
4. **Agregacje i podsumowania analityczne** – statystyki opisowe (`describe`, `summary`), grupowanie (`groupBy`) z wieloma funkcjami agregującymi (`agg`), tabele krzyżowe (`crosstab`).
5. **Optymalizacja wydajności, partycjonowanie i format Parquet** (*zbiór Employee*) – wpływ typów danych i liczby partycji (`repartition`, `coalesce`) na czas obliczeń, analiza planu wykonania Catalyst (`explain(mode="formatted")`), dyskretyzacja cech (`Bucketizer`) oraz zapis i odczyt w formacie kolumnowym **Apache Parquet**.

Każda sekcja notatnika została zbudowana według schematu: **prezentacja zagadnienia na praktycznym przykładzie $\rightarrow$ krótkie ćwiczenie utrwalające**, a całość zamyka **zadanie podsumowujące**.

---

## Uruchomienie środowiska pracy

Do realizacji laboratorium wykorzystujemy środowisko **PySpark 4.1.4 (Python 3.11 + Java 17/21)** skonfigurowane na poprzednich zajęciach ([Lab 5](../lab_05/readme.md)).

### Scenariusz 1: Lokalne środowisko wirtualne Pythona (`.venv_spark`)

1. Aktywuj środowisko wirtualne utworzone w `lab_05`:
   ```powershell
   # Windows PowerShell:
   .\.venv_spark\Scripts\Activate.ps1

   # Linux / macOS:
   source .venv_spark/bin/activate
   ```
2. Upewnij się, że zainstalowane są pakiety `numpy` (wymagany przez moduł `pyspark.ml.feature.Bucketizer`) oraz opcjonalnie `tqdm` (pasek postępu przy generowaniu zbioru testowego):
   ```powershell
   pip install numpy tqdm
   ```
3. Uruchom serwer Jupyter Lab w głównym katalogu repozytorium:
   ```powershell
   jupyter lab
   ```
4. W przeglądarce przejdź do folderu `lab_06` i otwórz notatnik **`02_pyspark_dataframes.ipynb`**.

---

### Scenariusz 2: Kontener Docker z `lab_05`

Jeśli korzystasz z kontenera Docker przygotowanego w `lab_05`:

1. Skopiuj notatnik `02_pyspark_dataframes.ipynb` do katalogu współdzielonego `lab_05/apps/`:
   ```powershell
   Copy-Item lab_06/02_pyspark_dataframes.ipynb lab_05/apps/
   ```
2. Uruchom kontener z poziomu katalogu `lab_05`:
   ```powershell
   cd lab_05
   docker compose up -d
   ```
3. Otwórz w przeglądarce adres **http://localhost:8888** i uruchom notatnik **`02_pyspark_dataframes.ipynb`**.
4. Po zakończeniu pracy możesz skopiować uzupełniony notatnik z powrotem do `lab_06/` i zatrzymać kontener (`docker compose down`).

---

## Monitorowanie zapytań DataFrame w Spark Web UI

Podczas wykonywania operacji na obiektach `DataFrame` zachęcamy do obserwowania interfejsu **Spark Web UI** pod adresem:
👉 **http://localhost:4040**

W porównaniu do niskopoziomowych operacji RDD, po uruchomieniu akcji na `DataFrame` w interfejsie Web UI pojawia się dodatkowa zakładka **SQL / DataFrame**, w której możesz wizualnie analizować:
* zoptymalizowany plan fizyczny zapytania (*Physical Plan*),
* liczbę wczytanych wierszy i rozmiar przefiltrowanych danych w krokach `FileScan csv` / `FileScan parquet`,
* koszt operacji przetasowania danych między partycjami (*Exchange / Shuffle*).

---

## Materiały laboratoryjne

Otwórz notatnik: **[02_pyspark_dataframes.ipynb](02_pyspark_dataframes.ipynb)**
