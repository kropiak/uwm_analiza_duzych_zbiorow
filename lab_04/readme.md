# Przetwarzanie dużych zbiorów danych

## Lab 4: Wykorzystanie biblioteki Dask w zadaniach Machine Learning.

W tej części zajęć przedstawione zostaną sposoby wykorzystania ekosystemu Dask (`dask-ml`, `joblib`, `xgboost.dask`) w uczeniu maszynowym:
* zrównoleglenie klasycznych algorytmów `scikit-learn` z użyciem backendu `joblib.parallel_backend('dask')`,
* natywny rozproszony trening modeli gradient boosting za pomocą `xgboost.dask`,
* uczenie przyrostowe (*Incremental learning* / *Out-of-Core*) na danych przekraczających pamięć RAM (`dask_ml.wrappers.Incremental`, `dask_ml.preprocessing.StandardScaler`),
* serializacja modeli (`joblib`) oraz rozproszona inferencja paczkowa z wykorzystaniem `Dask Futures`,
* rozproszona optymalizacja hiperparametrów metodą kolejnych odrzuceń (`dask_ml.model_selection.IncrementalSearchCV`) oraz klasyfikacja zespołowa na blokach danych (`dask_ml.ensemble.BlockwiseVotingClassifier`).

Przykłady i zadania znajdują się w notebooku: [lab_04.ipynb](lab_04.ipynb).
