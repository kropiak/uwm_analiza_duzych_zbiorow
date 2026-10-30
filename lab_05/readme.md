# Przetwarzanie dużych zbiorów danych

## Lab 5: Wstęp do Apache Spark

W tym laboratorium rozpoczynamy pracę z silnikiem **Apache Spark 4** – wiodącą platformą do rozproszonego i zrównoleglonego przetwarzania danych na dużą skalę.

W laboratorium przygotowano **dwa równorzędne scenariusze uruchomienia środowiska**. Możesz wybrać ten, który bardziej odpowiada Twojemu systemowi operacyjnemu i preferencjom.  
Niezależnie od wybranego scenariusza, **przykłady oraz zadania znajdują się w jednym wspólnym notatniku** [01_pyspark_introduction.ipynb](01_pyspark_introduction.ipynb), który w obu środowiskach działa w 100% identycznie.

---

### Wymagania wstępne dla Apache Spark 4.*

Apache Spark w gałęzi 4.x wprowadził istotne aktualizacje wymagań środowiskowych:
* **Java:** Wymagana jest **Java 17 (LTS)** lub **Java 21 (LTS)**. Starsze wersje (Java 8 i 11) zostały całkowicie wycofane.
* **Python:** Rekomendowaną, najbardziej stabilną wersją jest **Python 3.11** (minimalna wspierana wersja to Python 3.9, wspierany jest także Python 3.12).

---

## Scenariusz 1: Lokalne środowisko Pythona (Host / venv)

W tym scenariuszu PySpark uruchamiany jest bezpośrednio w wirtualnym środowisku Pythona na Twoim komputerze lokalnym. Nie wymaga on uruchamiania kontenerów Docker.

### Krok 1: Weryfikacja środowiska Javy
Spark do działania silnika JVM wymaga zainstalowanego środowiska Java 17 lub 21 oraz ustawionej zmiennej środowiskowej `JAVA_HOME`.
W terminalu (PowerShell / Bash) sprawdź:
```powershell
java -version
```
Upewnij się, że wersja to co najmniej `17.x` oraz że zmienna `JAVA_HOME` wskazuje na ścieżkę do katalogu JDK:
```powershell
# Windows PowerShell:
$env:JAVA_HOME

# Linux / macOS:
echo $JAVA_HOME
```

> Instalatory: https://www.openlogic.com/openjdk-downloads

### Krok 2: Utworzenie i aktywacja środowiska wirtualnego Python 3.11
W głównym katalogu projektu lub w folderze `lab_05` utwórz środowisko wirtualne oparte na interpreterze Pythona 3.11:

```powershell
# Windows:
py -3.11 -m venv .venv_spark
.\.venv_spark\Scripts\Activate.ps1

# Linux / macOS:
python3.11 -m venv .venv_spark
source .venv_spark/bin/activate
```

### Krok 3: Instalacja pakietów
Zainstaluj biblioteki wymagane do realizacji laboratorium (PySpark 4.0, JupyterLab oraz requests):
```powershell
pip install --upgrade pip
pip install -r lab_05/requirements.txt
```

### Krok 4: Uruchomienie Jupyter Lab
Uruchom serwer Jupyter Lab:
```powershell
jupyter lab
```
W przeglądarce przejdź do folderu `lab_05` i otwórz notatnik **`01_pyspark_introduction.ipynb`**.

> [!NOTE]
> **Użytkownicy systemu Windows:**  
> Podczas inicjalizacji SparkSession w konsoli może pojawić się ostrzeżenie `WARN Shell: Did not find winutils.exe`. Jest to w pełni bezpieczne ostrzeżenie powiązane z opcjonalnymi operacjami plikowymi Hadoop i nie ma żadnego wpływu na działanie zadań laboratoryjnych w pamięci RAM.

---

## Scenariusz 2: Kontener Docker (Spark 4)

W tym scenariuszu kompletny stos technologiczny (Spark 4, Java 17, Python oraz serwer Jupyter Lab) uruchamiany jest wewnątrz kontenera Linuksowego. Scenariusz ten jest idealny, jeśli nie chcesz instalować Javy 17 ani Pythona 3.11 na komputerze-hoście.

### Krok 1: Weryfikacja działania Docker Desktop
Upewnij się, że aplikacja **Docker Desktop** jest uruchomiona:
```powershell
docker --version
docker compose version
```

### Krok 2: Uruchomienie kontenera
Otwórz terminal w katalogu `lab_05` i wywołaj:
```powershell
docker compose up -d
```
Docker zbuduje obraz bazujący na `spark:4.1.4-scala2.13-java17-ubuntu`, zainstaluje środowisko Jupyter Lab i uruchomi kontener w tle.

### Krok 3: Dostęp do środowiska Jupyter Lab
Otwórz przeglądarkę internetową i wejdź pod adres:
👉 **http://localhost:8888**

Katalog roboczy kontenera (`/opt/spark/work-dir`) jest automatycznie połączony z lokalnym folderem `lab_05/apps/`. Znajdziesz w nim gotowy do pracy notatnik `01_pyspark_introduction.ipynb` oraz zbiór `pan-tadeusz.txt`.

### Krok 4: Zatrzymanie kontenera po zajęciach
Gdy zakończysz pracę, zatrzymaj kontener poleceniem:
```powershell
docker compose down
```
Wszystkie zmiany w plikach wewnątrz folderu `apps/` zostaną automatycznie zachowane na Twoim dysku lokalnym.

---

## Monitorowanie pracy Sparka (Web UI)

Zarówno w Scenariuszu 1, jak i w Scenariuszu 2 po zainicjalizowaniu obiektu `SparkSession` dostępny jest interfejs graficzny Spark Web UI:
👉 **http://localhost:4040**

W interfejsie możesz na bieżąco analizować:
* **Jobs & Stages:** zadania Sparka wywoływane przez akcje RDD (np. `take()`, `count()`, `reduce()`),
* **Executors:** liczbę wątków roboczych i zużycie pamięci,
* **Environment:** parametry konfiguracyjne JVM i PySpark.

---

## Przykłady i zadania do wykonania

Otwórz notatnik **`01_pyspark_introduction.ipynb`** (w lokalnym Jupyter Lab lub w przeglądarce kontenera pod adresem `localhost:8888`).

