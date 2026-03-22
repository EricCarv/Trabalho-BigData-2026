#import setuptools
import os
from pyspark.sql import SparkSession

os.environ["JAVA_HOME"] = r"C:\Program Files\Java\jdk-17"
os.environ["PATH"] += os.pathsep + os.path.join(os.environ["JAVA_HOME"], "bin")
os.environ["HADOOP_HOME"] = r"C:\hadoop\bin"


spark = SparkSession.builder.appName("Trabalho de Big DATA").config("spark.driver.extraJavaOptions", "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED").config("spark.executor.extraJavaOptions", "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED").config("spark.memory.offHeap.enabled","true").config("spark.memory.offHeap.size","2g").getOrCreate()
df=spark.read.csv("archive/imdb-reviews-pt-br.csv")
df.show(5,0)
