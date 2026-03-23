import os
import sys
from pyspark.sql import SparkSession

os.environ["JAVA_HOME"] = r"C:\Program Files\Java\jdk-17"
os.environ["PATH"] += os.pathsep + os.path.join(os.environ["JAVA_HOME"], "bin")
os.environ["HADOOP_HOME"] = r"C:\hadoop\bin"
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

RU=3602977

spark = SparkSession.builder.appName("Trabalho de Big DATA").config("spark.driver.extraJavaOptions", "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED")\
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

'''
print("passou")
df.show(5,0)
print("passou")
'''

def map1(x):
    try:
        return x['sentiment'], int(x['id'])
    except:
        return x['sentiment'], 0

def reduceByKey1(x,y):
    return x+y

resultadoreduce=df.rdd.map(map1).reduceByKey(reduceByKey1).collect()

for sentimento, soma in resultadoreduce:
    if sentimento == "neg":
        print(f"\n Sentimento selecionado: {sentimento}")
        print(f"Soma dos IDs de linhas com sentimento NEG: {soma}")

print("resultadoreduce:",resultadoreduce)



print(f"RU:",RU)
print()