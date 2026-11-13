# Przetwarzanie Dużych Zbiorów Danych – Inspiracje i wytyczne do projektów (Semestr 2026Z)

Niniejszy dokument zawiera **5 zrównoważonych pod względem pracochłonności ścieżek projektowych** wraz z alternatywnymi zbiorami danych oraz ujednoliconym standardem realizacji.

Głównym celem projektu jest zaprojektowanie i wdrożenie **solidnego, powtarzalnego potoku inżynierii danych (Data Engineering)** z wykorzystaniem narzędzi poznanych na zajęciach (**Apache Spark / PySpark** lub **Dask**), formatów zoptymalizowanych (**Parquet / Delta Lake**) oraz prezentacja wyników biznesowych w prostym dashboardzie (np. **Streamlit**, wbudowany pulpit w darmowym **Databricks Community Edition** lub wykresy interaktywne).

---

## 1. Zunifikowany standard realizacji projektu (4 Etapy)

Niezależnie od wybranej ścieżki tematycznej i źródła danych, każdy projekt oceniany jest według tego samego, 4-etapowego schematu architektury medalionowej:

```
[ Źródło Danych ] 
       │
       ▼
[ Etap 1: Warstwa Bronze ]  ──> Zapis surowych danych do Parquet/Delta + podział na partycje
       │
       ▼
[ Etap 2: Warstwa Silver ]  ──> Deduplikacja, ujednolicenie typów, czyszczenie, relacje tabel
       │
       ▼
[ Etap 3: Warstwa Gold ]    ──> Agregacje biznesowe, KPI, kalkulacje analityczne, rankingi
       │
       ▼
[ Etap 4: Prezentacja ]     ──> Lekki dashboard (Streamlit / Databricks) + raport i wnioski
```

### Etap 1: Ingest & Raw (Warstwa Bronze)
* **Zadanie:** Pobranie danych ze wskazanego źródła (API, otwartoźródłowe repozytoria, portale Open Data, Kaggle, Hugging Face) o reprezentatywnym wolumenie (od kilkuset tysięcy do kilku milionów rekordów / setki MB–kilka GB).
* **Efekt techniczny:** Konwersja surowych plików (CSV, JSON, XML) do zoptymalizowanego formatu kolumnowego (**Parquet** lub **Delta Lake**) z podziałem na logiczne partycje (np. według roku/miesiąca lub kategorii) oraz dodaniem metadanych wczytania (`ingestion_timestamp`).

### Etap 2: Clean & Model (Warstwa Silver)
* **Zadanie:** Rozproszone czyszczenie, walidacja i łączenie danych w PySpark lub Dask.
* **Efekt techniczny:**
  * Usunięcie duplikatów i niepoprawnych rekordów,
  * Konwersja i ujednolicenie typów danych (daty, znaczniki czasu, wartości numeryczne),
  * Złączenia relacyjne (np. tabela faktów z wymiarami: zamówienie $\leftrightarrow$ klient $\leftrightarrow$ produkt, współrzędne $\leftrightarrow$ region),
  * Zapis oczyszczonej warstwy Silver do dedykowanego katalogu/tabeli.

### Etap 3: Business Analytics & Aggregations (Warstwa Gold)
* **Zadanie:** Obliczenie kluczowych wskaźników biznesowych (KPI), agregacji przekrojowych lub metryk w oknach czasowych.
* **Efekt techniczny:** Wygenerowanie 3–5 zwięzłych tabel zagregowanych (np. sprzedaż per region i kwartał, wskaźniki awaryjności, anomalie w ruchu sieciowym, rozkład sentymentu) i utrwalenie ich w formacie Gold.

### Etap 4: Dashboard, raport i wnioski
* **Zadanie:** Wizualizacja wyników z warstwy Gold oraz udokumentowanie projektu.
* **Efekt techniczny:**
  * Prosty, estetyczny dashboard analityczny: aplikacja w **Streamlit** (można uruchomić lokalnie lub bezpłatnie na *Streamlit Community Cloud*), wbudowany **Databricks Lakehouse Dashboard**, lub notatnik z interaktywnymi wykresami Plotly,
  * Krótki raport w pliku `README.md` w repozytorium opisujący architekturę potoku, napotkane wyzwania optymalizacyjne (np. unikanie *data skew*, dobór partycji) oraz wnioski z danych.

---

## 2. Katalog 5 ścieżek tematycznych do wyboru

Każda ścieżka posiada tę samą wagę punktową. Wybierz jeden z poniższych tematów oraz jeden ze wskazanych zbiorów danych.

---

### Ścieżka 1: E-Commerce & Retail – Architektura Medallion
* **Główny profil:** Klasyczna inżynieria danych dla relacyjnych transakcji handlowych.
* **Rekomendowane środowisko:** Databricks Community Edition (Delta Lake) LUB lokalny PySpark (Parquet).
* **Zadania w warstwach:**
  * *Bronze:* Ingest wielotabelowych zbiorów transakcyjnych z zachowaniem surowej postaci.
  * *Silver:* Złączenie tabel relacyjnych (zamówienia $\leftrightarrow$ pozycje zamówień $\leftrightarrow$ płatności $\leftrightarrow$ recenzje), wyliczenie rzeczywistych czasów realizacji dostaw, czyszczenie anomalii cenowych.
  * *Gold:* Analiza kohortowa klientów (Customer Retention), segmentacja RFM (Recency, Frequency, Monetary Value), wskaźniki anulacji i opóźnień per region.
  * *Dashboard:* Panel menedżerski pokazujący dynamikę przychodów, mapę dostaw i top kategorie produktów.
* **Sugerowane zbiory danych (wybierz jeden):**
  1. **Olist Brazilian E-Commerce Dataset (Kaggle):** Ponad 100 000 prawdziwych zamówień z lat 2016–2018 w 9 połączonych tabelach relacyjnych.  
     🔗 *Link:* https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce
  2. **Instacart Market Basket Analysis (Kaggle):** Ponad 3 miliony zamówień z 200 tysięcy użytkowników z historią produktów dodawanych do koszyka.  
     🔗 *Link:* https://www.kaggle.com/c/instacart-market-basket-analysis/data
  3. **H&M Personalized Fashion Recommendations (Kaggle):** 31 milionów wierszy transakcji zakupowych z bogatymi atrybutami artykułów i klientów.  
     🔗 *Link:* https://www.kaggle.com/competitions/h-and-m-personalized-fashion-recommendations/data
  4. **Otwarte dane Allegro / Ceneo:** Wykaz ofert, historii cen lub opinii konsumenckich dostępnych na platformie Hugging Face / Kaggle.

---

### Ścieżka 2: Smart City & Otwarte Dane Publiczne (Gov Data)
* **Główny profil:** Przetwarzanie i integracja danych przestrzenno-czasowych z sektora publicznego.
* **Rekomendowane środowisko:** PySpark (z funkcjami dat i okien) LUB Dask DataFrame.
* **Zadania w warstwach:**
  * *Bronze:* Wczytanie pomiarów lub zdarzeń miejskich z publicznych API lub zrzutów danych do Parquet.
  * *Silver:* Geokodowanie/standaryzacja jednostek terytorialnych (kody TERYT, dzielnice, województwa), czyszczenie anomalii pomiarowych, obsługa braków w ciągach czasowych.
  * *Gold:* Agregacje punktualności transportu w szczytach komunikacyjnych, mapy korelacji zanieczyszczeń powietrza z porami doby i warunkami pogodowymi, średni czas oczekiwania na świadczenia medyczne.
  * *Dashboard:* Interaktywna mapa punktów zapalnych (np. w Streamlit z biblioteką Pydeck lub Folium) z suwakami czasu.
* **Sugerowane zbiory danych (wybierz jeden):**
  1. **Otwarte Dane m.st. Warszawy (`api.um.warszawa.pl`):** Pozycje GPS autobusów i tramwajów w czasie rzeczywistym, rozkłady jazdy, baza zgłoszeń mieszkańców Warszawa 19115.  
     🔗 *Link:* https://api.um.warszawa.pl
  2. **Główny Inspektorat Ochrony Środowiska (GIOŚ / dane.gov.pl):** Wielomilionowe szeregi czasowe pomiarów stacji jakości powietrza (pyły PM10, PM2.5, NO2, O3) z lat 2018–2024.  
     🔗 *Link:* https://dane.gov.pl/pl/dataset/1749,pomiary-jakosci-powietrza
  3. **Narodowy Fundusz Zdrowia (NFZ / dane.gov.pl):** Ogólnopolska baza danych o czasie oczekiwania na świadczenia medyczne i kolejkach do specjalistów.  
     🔗 *Link:* https://dane.gov.pl/pl/dataset/2085,terminy-leczenia-czas-oczekiwania-na-swiadczenia
  4. **NYC Taxi & Limousine Commission (TLC) Trip Record Data:** Klasyczny zbiór referencyjny Big Data z wieloma milionami przejazdów taksówek z dokładnymi współrzędnymi GPS.  
     🔗 *Link:* https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

---

### Ścieżka 3: Energia, Pogoda i Szeregi Czasowe
* **Główny profil:** Analiza i fuzja równoległych szeregów czasowych z sektora elektroenergetycznego i meteorologicznego.
* **Rekomendowane środowisko:** Dask DataFrame (świetnie radzi sobie z szeregami czasowymi) LUB PySpark.
* **Zadania w warstwach:**
  * *Bronze:* Ingest danych o zapotrzebowaniu i generacji OZE oraz danych ze stacji meteorologicznych.
  * *Silver:* Resampling i dopasowanie kroków czasowych (godzina/doba), ujednolicenie stref czasowych (UTC vs CET/CEST), imputacja pojedynczych braków pomiarowych.
  * *Gold:* Wyliczenie współczynników korelacji (temperatura $\leftrightarrow$ zapotrzebowanie na moc chłodniczą/grzewczą, siła wiatru $\leftrightarrow$ generacja z wiatraków), analiza skrajnych odchyleń od planów dobowych.
  * *Dashboard:* Pulpit monitorowania bilansu sieci z wykresami trendów, wskaźnikami udziału OZE w miksie energetycznym.
* **Sugerowane zbiory danych (wybierz jeden):**
  1. **Polskie Sieci Elektroenergetyczne (PSE API / pliki raportowe):** Godzinowe zapotrzebowanie KSE, generacja wiatrowa i fotowoltaiczna w Polsce.  
     🔗 *Link:* https://www.pse.pl/dane-systemowe
  2. **Open-Meteo Historical Weather API:** Darmowe, nielimitowane API z historycznymi danymi meteorologicznymi dla dowolnych współrzędnych w Polsce.  
     🔗 *Link:* https://open-meteo.com/en/docs/historical-weather-api
  3. **ENTSO-E Transparency Platform (Kaggle / Portal ENTSO-E):** Europejskie dane o cogodzinnych cenach energii, generacji i przepływach transgranicznych.  
     🔗 *Link:* https://transparency.entsoe.eu
  4. **NOAA Global Hourly Surface Data:** Globalne dane pomiarowe ze stacji meteorologicznych z wielu lat.  
     🔗 *Link:* https://www.ncei.noaa.gov/products/land-based-station/integrated-surface-database

---

### Ścieżka 4: Przetwarzanie i Ekstrakcja Wiedzy z Polskich Tekstów (Big Data NLP)
* **Główny profil:** Rozproszone przetwarzanie języka naturalnego (NLP) na dużych korpusach w języku polskim (bez kosztownych LLM).
* **Rekomendowane środowisko:** PySpark (z modułami `pyspark.ml.feature` i wyrażeniami regularnymi) LUB DuckDB / Dask.
* **Zadania w warstwach:**
  * *Bronze:* Załadowanie surowego korpusu tekstów (np. z Hugging Face Datasets w formacie JSON/Parquet).
  * *Silver:* Rozproszony potok czyszczenia: usunięcie znaków specjalnych/HTML, tokenizacja słów, filtracja polskich słów stop (*stopwords*), lematyzacja/stemming, ekstrakcja metadanych tekstu (długość, statystyki słownictwa).
  * *Gold:* Budowa reprezentacji wektorowej TF-IDF (`HashingTF` + `IDF`), identyfikacja najpopularniejszych słów-kluczy per kategoria/źródło, prosty rozproszony model klasyfikacyjny (np. `LogisticRegression` ze Spark MLlib) lub zliczanie wskaźników sentymentu.
  * *Dashboard:* Interaktywne narzędzie do wpisania/analizy tekstu, chmury kluczowych słów per kategoria, podgląd statystyk korpusu.
* **Sugerowane zbiory danych (wybierz jeden):**
  1. **PolEmo 2.0 (`clarin-pl/polemo2-official` na Hugging Face):** Oficjalny polski korpus opinii konsumenckich (hotele, medycyna, produkty, uczelnie) z adnotacjami wydźwięku.  
     🔗 *Link:* https://huggingface.co/datasets/clarin-pl/polemo2-official
  2. **Allegro Reviews (`allegro_reviews` na Hugging Face):** Korpus setek tysięcy polskich recenzji produktów z przypisaną oceną od 1 do 5 gwiazdek.  
     🔗 *Link:* https://huggingface.co/datasets/allegro_reviews
  3. **Otwarte Dane Sejmu RP (`api.sejm.gov.pl`):** Pełne transkrypcje wypowiedzi poselskich, interpelacji i debat sejmowych z IX i X kadencji.  
     🔗 *Link:* https://api.sejm.gov.pl
  4. **Zbiór artykułów prasowych Onet / WP News Dataset (Hugging Face / Kaggle):** Dziesiątki tysięcy artykułów informacyjnych w języku polskim podzielonych na kategorie tematyczne.

---

### Ścieżka 5: Analityka Logów Serwerowych, Telemetria i Bezpieczeństwo
* **Główny profil:** Analiza masowych logów dostępowych i zdarzeń sieciowych pod kątem wydajności i wykrywania anomalii.
* **Rekomendowane środowisko:** PySpark Core & SQL (funkcje `regexp_extract`, okna analityczne).
* **Zadania w warstwach:**
  * *Bronze:* Załadowanie surowych plików tekstowych w formacie Common Log Format (Apache/Nginx) lub strumieni zdarzeń JSON.
  * *Silver:* Ekstrakcja pól za pomocą wyrażeń regularnych: adres IP, timestamp, metoda HTTP, ścieżka URI, kod odpowiedzi (200, 404, 500), wielkość odpowiedzi, User-Agent; mapowanie IP na kraje/organizacje.
  * *Gold:* Detekcja anomalii: identyfikacja agresywnych skanerów i prób ataków brute-force (np. adresy IP z nienaturalną liczbą żądań w oknie 1-minutowym), zestawienie zasobów generujących najwięcej błędów 4xx/5xx, analiza wąskich gardeł transferu danych.
  * *Dashboard:* Pulpit administratora / bezpieczeństwa: wykresy ruchu w czasie, lista podejrzanych adresów IP do zablokowania, rozkład geograficzny użytkowników.
* **Sugerowane zbiory danych (wybierz jeden):**
  1. **Loghub (GitHub – zbiory logów systemowych):** Bogata kolekcja logów z serwerów Apache, klastrów Hadoop, Spark, systemów Linux i HDFS (od setek MB do dziesiątek GB).  
     🔗 *Link:* https://github.com/logpai/loghub
  2. **NASA Kennedy Space Center Server Logs:** Klasyczny zbiór zawierający wszystkie zapytania HTTP do serwera NASA z dwóch miesięcy (miliony rekordów).  
     🔗 *Link:* https://ita.ee.lbl.gov/html/contrib/NASA-HTTP.html
  3. **CIC-IDS2017 Network Intrusion Detection (Kaggle):** Zbiór ruchu sieciowego zawierający zarejestrowane ataki (DDoS, Brute Force, Web Attacks) zmieszane z normalnym ruchem.  
     🔗 *Link:* https://www.kaggle.com/datasets/cicdataset/cicids2017
  4. **GitHub Archive (GH Archive):** Publiczne zdarzenia z platformy GitHub (commity, pull requesty, zgłoszenia issue) zapisane w formacie JSON z każdego dnia.  
     🔗 *Link:* https://www.gharchive.org

---

## 3. Praktyczne wskazówki dla studentów

> [!TIP]
> **1. Praca na próbce vs pełnym zbiorze:**  
> W fazie budowy potoku i testowania kodu pracuj na małej próbce danych (np. 10 000 wierszy lub jeden plik dzienny). Dopiero po upewnieniu się, że cały potok wykonuje się bezbłędnie, uruchom go na pełnym wolumenie danych.
>
> **2. Unikaj antywzorców Big Data:**  
> Pamiętaj, że wywołanie `.toPandas()` lub `.collect()` na wielogigabajtowym zbiorze spowoduje natychmiastowe przepełnienie pamięci RAM sterownika (**OutOfMemoryError**). Konwersję do Pandas wykonuj wyłącznie na silnie zagregowanych danych w warstwie Gold tuż przed przekazaniem ich do wykresu w dashboardzie!
>
> **3. Optymalizacja partycjonowania:**  
> Zadbaj o to, aby partycje w formacie Parquet nie były ani zbyt małe (problem tysięcy plików po kilka kilobajtów tzw. *Small Files Problem*), ani zbyt duże (powyżej 500 MB na partycję). W razie potrzeby użyj `coalesce()` po operacjach filtrowania.
>
> **4. Format oddania:**  
> Rozwiązanie należy dostarczyć w formie repozytorium Git (GitHub / GitLab) zawierającego:
> * Kod źródłowy (notatniki `.ipynb` lub skrypty `.py`),
> * Instrukcję uruchomienia środowiska w pliku `README.md`,
> * Plik `requirements.txt` lub definicję środowiska,
> * Zrzuty ekranu lub link do działającego dashboardu.

---

## 4. Szablon karty zgłoszenia tematu

Przed przystąpieniem do realizacji projektu każdy zespół/student przesyła krótkie zgłoszenie tematu wg poniższego wzoru:

```markdown
### Karta Zgłoszenia Projektu
1. **Skład zespołu:** (Imię, nazwisko, nr albumu)
2. **Wybrana ścieżka:** (np. Ścieżka 2: Smart City & Otwarte Dane Publiczne)
3. **Źródło i nazwa zbioru danych:** (np. GIOŚ – Pomiary jakości powietrza w latach 2020-2024, dane.gov.pl)
4. **Wykorzystane narzędzia Big Data:** (np. PySpark 4.1.4, format Parquet z partycjonowaniem po roku i stacji)
5. **Planowana forma prezentacji:** (np. Aplikacja w Streamlit z mapą stacji pomiarowych)
```
