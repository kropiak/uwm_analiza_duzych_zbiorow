# Przetwarzanie dużych zbiorów danych

## Lab 7: PySpark i Nowoczesna Hurtownia Danych (Delta Lakehouse – część 1)

W tym laboratorium przechodzimy od przetwarzania plików (`DataFrame` i `Parquet` z Lab 6) do budowy **nowoczesnej hurtowni danych typu Lakehouse** w oparciu o **Apache Spark 4.1** oraz **Delta Lake 4.4**.

Temat jest podzielony na dwa kolejne spotkania:
* **Lab 7 (część 1):** Spark SQL, ograniczenia „surowego” formatu Parquet (*Data Swamp*), transakcje ACID i anatomia `_delta_log`, operacje DML (`UPDATE`, `DELETE`, `MERGE INTO`), *Time Travel* i `RESTORE`, *Schema Enforcement / Evolution* oraz budowa warstw **Bronze $\rightarrow$ Silver** w architekturze medalionowej.
* **Lab 8 (część 2 – kolejne zajęcia):** modelowanie wymiarowe (*Star Schema*), *Slowly Changing Dimensions Type 2* (`SCD2`), warstwa analityczna **Gold**, optymalizacja fizyczna (`OPTIMIZE`, *Liquid Clustering* `CLUSTER BY`, `VACUUM`) oraz lekka konsumpcja danych przez **DuckDB**.

---

## Wybór środowiska uruchomieniowego

> [!IMPORTANT]
> **Rekomendacja dla zajęć w laboratorium: GitHub Codespaces**  
> Zapis tabel Delta Lake (atomowe operacje na katalogu `_delta_log`, `MERGE`, `VACUUM`) wymaga poprawnej obsługi warstwy plikowej Hadoop. W systemie Windows lokalne uruchomienie bez natywnych binariów `winutils.exe` i `hadoop.dll` potrafi powodować błędy zapisu, a na komputerach uczelnianych często brakuje uprawnień do instalacji Javy 17.  
> **GitHub Codespaces** uruchamia gotowe, ujednolicone środowisko Linuksowe w przeglądarce z preinstalowaną Javą 17, Pythonem 3.11, PySparkiem 4.1.4, Delta Lake 4.4.1 oraz **wstępnie zapisanym cache JAR-ów** (start bez czekania na pobieranie zależności).

### Scenariusz 1 (Rekomendowany): GitHub Codespaces

1. Na stronie głównej repozytorium na GitHubie kliknij zielony przycisk **`<> Code`** $\rightarrow$ zakładka **`Codespaces`** $\rightarrow$ **`Create codespace on ...`** (domyślnie uruchamia konfigurację z `.devcontainer/devcontainer.json`).
2. Po załadowaniu środowiska w przeglądarce otwórz plik **`lab_07/01_delta_lakehouse_medallion.ipynb`** bezpośrednio w edytorze VS Code (wybierz kernel Python 3.11) **lub** uruchom klasyczny JupyterLab w terminalu Codespace:
   ```bash
   jupyter lab --ip=0.0.0.0 --port=8888 --no-browser
   ```
   i otwórz przekierowany port **8888** w zakładce *Ports*.
3. Pamiętaj o limitach darmowego planu GitHub Free (120 core-hours/miesiąc, czyli ok. 60 h na maszynie 2-core). Po zakończeniu pracy zatrzymaj Codespace (*Stop Current Codespace*), aby nie zużywać minut w tle (środowisko usypia się też automatycznie po 30 min bezczynności).

---

### Scenariusz 2: Lokalny kontener Docker (fallback)

Jeśli pracujesz na własnym komputerze z zainstalowanym **Docker Desktop**:

1. Przejdź do katalogu `lab_07` i uruchom kontener (obraz bazuje na `spark:4.1.4-scala2.13-java17-ubuntu` i zawiera już cache JAR-ów Delta Lake):
   ```powershell
   cd lab_07
   docker compose up -d
   ```
2. Otwórz w przeglądarce: **http://localhost:8888**, przejdź do `01_delta_lakehouse_medallion.ipynb`.
3. Po zakończeniu pracy zatrzymaj kontener:
   ```powershell
   docker compose down
   ```

---

### Scenariusz 3: Lokalne środowisko Pythona (Linux / macOS / własny Windows z Hadoop winutils)

1. Upewnij się, że używasz **Java 17 lub 21** (`java -version`) oraz **Pythona 3.11** (np. środowisko `.venv_spark` z Lab 5).
2. Zainstaluj pakiety wymagane dla Lab 7:
   ```bash
   pip install -r lab_07/requirements_delta.txt
   ```
3. Uruchom `jupyter lab` w katalogu głównym lub `lab_07/` i otwórz notatnik **`01_delta_lakehouse_medallion.ipynb`**.

> [!NOTE]
> **Użytkownicy systemu Windows (Scenariusz 3):**  
> Jeśli przy zapisie tabeli Delta pojawi się błąd `UnsatisfiedLinkError` związany z `NativeIO$Windows.access0` lub brakującym `winutils.exe`, skorzystaj ze Scenariusza 1 (Codespaces) lub Scenariusza 2 (Docker).

---

## Monitorowanie pracy Sparka (Spark Web UI)

Po uruchomieniu sesji `SparkSession` możesz obserwować plany zapytań, zadania SQL i operacje Delta Lake pod adresem:
👉 **http://localhost:4040** (w Codespaces adres pojawi się automatycznie w zakładce **Ports** przy porcie `4040`).

---

## Materiały

* Notatnik główny: **[01_delta_lakehouse_medallion.ipynb](01_delta_lakehouse_medallion.ipynb)**
* Generator danych e-commerce: **[ecommerce_data_gen.py](ecommerce_data_gen.py)**
* Archiwum poprzedniej wersji laboratorium (Hive): [`__old/`](__old/)
