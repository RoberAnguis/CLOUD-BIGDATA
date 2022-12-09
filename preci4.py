#mm/dia al año. Ojo! mm = L/m^2, es decir, 1000 mL/m^2. Si lo queréis en esta otra unidad decidmelo y cambio el código

import os
import sys
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType,StructField, StringType, DoubleType, IntegerType

conf = SparkConf().setAppName('ProyectoSpark')
sc = SparkContext(conf = conf)

spark = SparkSession.builder.appName('ProyectoSpark').getOrCreate()

schema = StructType([
	StructField("Año",StringType(),True),
	StructField("Mes",StringType(),True),
	StructField("Modelo",StringType(),True),
	StructField("Punto",StringType(),True),
	StructField("Longitud",DoubleType(),True),
	StructField("Latitud",DoubleType(),True),
	StructField("Día 1",DoubleType(),True),
	StructField("Día 2",DoubleType(),True),
	StructField("Día 3",DoubleType(),True),
	StructField("Día 4",DoubleType(),True),
	StructField("Día 5",DoubleType(),True),
	StructField("Día 6",DoubleType(),True),
	StructField("Día 7",DoubleType(),True),
	StructField("Día 8",DoubleType(),True),
	StructField("Día 9",DoubleType(),True),
	StructField("Día 10",DoubleType(),True),
	StructField("Día 11",DoubleType(),True),
	StructField("Día 12",DoubleType(),True),
	StructField("Día 13",DoubleType(),True),
	StructField("Día 14",DoubleType(),True),
	StructField("Día 15",DoubleType(),True),
	StructField("Día 16",DoubleType(),True),
	StructField("Día 17",DoubleType(),True),
	StructField("Día 18",DoubleType(),True),
	StructField("Día 19",DoubleType(),True),
	StructField("Día 20",DoubleType(),True),
	StructField("Día 21",DoubleType(),True),
	StructField("Día 22",DoubleType(),True),
	StructField("Día 23",DoubleType(),True),
	StructField("Día 24",DoubleType(),True),
	StructField("Día 25",DoubleType(),True),
	StructField("Día 26",DoubleType(),True),
	StructField("Día 27",DoubleType(),True),
	StructField("Día 28",DoubleType(),True),
	StructField("Día 29",DoubleType(),True),
	StructField("Día 30",DoubleType(),True),
	StructField("Día 31",DoubleType(),True),
]) 

#Creamos el dataframe      
df = spark.read.option("header", "true").option("sep",";").schema(schema).csv("Preci4.csv") #para RCP 8.5, usar "Preci8.csv"
#Media diaria (esto se podría usar luego si queremos sacarlo como dato propio)
df_day = df.groupBy("Año","Mes").avg('Día 1','Día 2','Día 3','Día 4','Día 5','Día 6','Día 7','Día 8','Día 9','Día 10','Día 11','Día 12','Día 13','Día 14','Día 15','Día 16','Día 17','Día 18','Día 19','Día 20','Día 21','Día 22','Día 23','Día 24','Día 25','Día 26','Día 27','Día 28','Día 29','Día 30','Día 31').sort('Año', 'Mes')

#Media anual
def avg_preci_year(line): 
    suma = 0
    days_of_month = 0
    for i in range (2, 33):
        if (line[i] != None):
            suma = suma + line[i]
            days_of_month = days_of_month + 1

    return (line[0], (line[1], suma, days_of_month))


#Reconversión a df y guardado en un csv
avg_preci = df_day.rdd.map(avg_preci_year).reduceByKey(lambda a,b: ("basura", (a[1]+b[1]), (a[2]+b[2]))).map(lambda x: (x[0], x[1][1]/x[1][2]))
df_final = spark.createDataFrame(avg_preci).toDF("Año","Avg_Preci")
df_final.write.mode("overwrite").option("header", "true").option("sep",";").csv("AvgPreci_4.csv")

