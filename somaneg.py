#import setuptools
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

os.environ["JAVA_HOME"] = r"C:\Program Files\Java\jdk-17"
os.environ["PATH"] += os.pathsep + os.path.join(os.environ["JAVA_HOME"], "bin")
os.environ["HADOOP_HOME"] = r"C:\hadoop\bin"

RU=3602977

spark = SparkSession.builder.appName("Trabalho de Big DATA").config("spark.driver.extraJavaOptions", "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED").config("spark.executor.extraJavaOptions", "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED").config("spark.memory.offHeap.enabled","true").config("spark.memory.offHeap.size","2g").getOrCreate()

df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("multiLine", "true") \
    .option("escape", "\"") \
    .option("quote", "\"") \
    .load("archive/imdb-reviews-pt-br.csv")
df.show(5,0)

dfneg = df.filter(df.sentiment == "neg")
print(dfneg.count())
somaIDneg = dfneg.select(F.sum("id")).collect()[0][0]

print(somaIDneg)