from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split


# inicjalizacja sesji Spark
spark = SparkSession \
    .builder \
    .appName("StructuredStreamingExample") \
    .getOrCreate()

# domyślnie log level to INFO, które dość mocno spamuje okno konsoli
# możemy więc zmienić na nasze potrzeby do poziomu WARN
spark.sparkContext.setLogLevel('WARN')

# tworzymy DataFrame reprezentujący strumień linii z połączenia do localhost:9999
# w dokumentacji znajdziemy też informację, że format socket jest wykorzystywany tylko
# do testowania

lines = spark \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# dzielenie linii na słowa
words = lines.select(
   explode(
       split(lines.value, " ")
   ).alias("word")
)

# uruchomienie mechanizmu dzielenia tekstu na słowa
wordCounts = words.groupBy("word").count()

# uruchomienie zapytania, które będzie wyświetlało liczbę słów na konsolę
query = wordCounts \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()