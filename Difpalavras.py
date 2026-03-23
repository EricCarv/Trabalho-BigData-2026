import os
import sys
from pyspark.sql import SparkSession



os.environ["JAVA_HOME"] = r"C:\Program Files\Java\jdk-17"
os.environ["PATH"] += os.pathsep + os.path.join(os.environ["JAVA_HOME"], "bin")
os.environ["HADOOP_HOME"] = r"C:\hadoop\bin"
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

RU=3602977

spark = SparkSession.builder.appName("Trabalho de Big DATA questao 2").config("spark.driver.extraJavaOptions", "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED")\
    .config("spark.executor.extraJavaOptions", "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED")\
    .config("spark.memory.offHeap.enabled","true")\
    .config("spark.memory.offHeap.size","2g")\
    .getOrCreate()

df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("multiLine", "true") \
    .option("escape", "\"") \
    .option("quote", "\"") \
    .load("archive/imdb-reviews-pt-br.csv")

def map2(x):
    if x['sentiment'] == 'neg':
        try:
            qtd_en = len(x['text_en'].split())
            qtd_br = len(x['text_pt'].split())

            diferenca = qtd_br - qtd_en
            return x['sentiment'],diferenca
        except:
            return x['sentiment'],0
    else:
        return x['sentiment'],0
def reduceByKey2(x, y):

    return x+y

resultadoreduce = df.rdd.filter(lambda x: x['sentiment'] == 'neg').map(map2).reduceByKey(reduceByKey2).collect()

for sentimento, diferenca in resultadoreduce:
    if sentimento == "neg":
        print(f"Diferença no numero de palavras entre textos em portugues e ingles: {diferenca}")
print(resultadoreduce)
print(f"RU",RU)
