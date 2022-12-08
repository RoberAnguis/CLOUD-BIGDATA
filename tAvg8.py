


from pyspark import SparkConf, SparkContext

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, DoubleType, IntegerType
from pyspark.sql.functions import *
from decimal import Decimal

import sys
import re

conf = SparkConf().setAppName('ProyectoSpark')
sc = SparkContext(conf = conf)

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

spark = SparkSession.builder.appName('ProyectoSpark').getOrCreate()

df = spark.read.option("header", "true").option("sep",";").schema(schema).csv("Tmax8.csv")
dfMin = spark.read.option("header", "true").option("sep",";").schema(schema).csv("Tmin8.csv")

#Avg por año y mes
df2 = df.groupBy("Año", "Mes").agg({'Día 1':'avg', 'Día 2':'avg', 'Día 3':'avg', 'Día 4':'avg', 'Día 5':'avg', 'Día 6':'avg', 'Día 7':'avg', 'Día 8':'avg', 'Día 9':'avg', 'Día 10':'avg', 'Día 11':'avg', 'Día 12':'avg', 'Día 13':'avg', 'Día 14':'avg', 'Día 15':'avg', 'Día 16':'avg', 'Día 17':'avg', 'Día 18':'avg', 'Día 19':'avg', 'Día 20':'avg', 'Día 21':'avg', 'Día 22':'avg', 'Día 23':'avg', 'Día 24':'avg', 'Día 25':'avg', 'Día 26':'avg', 'Día 27':'avg', 'Día 28':'avg', 'Día 29':'avg', 'Día 30':'avg', 'Día 31':'avg'}).sort("Año", "Mes")
dfMin2 = dfMin.groupBy("Año", "Mes").agg({'Día 1':'avg', 'Día 2':'avg', 'Día 3':'avg', 'Día 4':'avg', 'Día 5':'avg', 'Día 6':'avg', 'Día 7':'avg', 'Día 8':'avg', 'Día 9':'avg', 'Día 10':'avg', 'Día 11':'avg', 'Día 12':'avg', 'Día 13':'avg', 'Día 14':'avg', 'Día 15':'avg', 'Día 16':'avg', 'Día 17':'avg', 'Día 18':'avg', 'Día 19':'avg', 'Día 20':'avg', 'Día 21':'avg', 'Día 22':'avg', 'Día 23':'avg', 'Día 24':'avg', 'Día 25':'avg', 'Día 26':'avg', 'Día 27':'avg', 'Día 28':'avg', 'Día 29':'avg', 'Día 30':'avg', 'Día 31':'avg'}).sort("Año", "Mes")


def average(line): #para cada tupla año,mes calcula la media de temperatura
	sum = 0
	cont = 0
	for i in range (2, 33):#creo que estan bien los rangos
		if (line[i] != None):
			sum += Decimal(line[i])
			cont += 1
	return (line[0], (line[1], sum/cont))

			
	

#Hacer el avg de cada día
t_max_avg_8 = df2.rdd.map(average).reduceByKey(lambda a,b: ("basura", (a[1]+b[1]))).map(lambda x: (x[0], x[1][1]/12))
t_max_avg_8.collect()

t_min_avg_8 = dfMin2.rdd.map(average).reduceByKey(lambda a,b: ("basura", (a[1]+b[1]))).map(lambda x: (x[0], x[1][1]/12))
t_min_avg_8.collect()



''' Ejemplo:

Año	 Avg

2006     34
2007     14
.
.
.
2020     23
.
.
.
2100     27
 '''

#hacer lo mismo con Tmin y hacer la media entre ambos

rdd_final = t_max_avg_8.join(t_min_avg_8)
rdd_final.collect()

def mediaMinMax(line):
	media = 0
	media = (line[1][0] + line[1][1])/2
	return (line[0], media)

rdd_ final = rdd_final.map(mediaMinMax)
rdd_final.collect()


df_res = spark.createDataFrame(rdd_final).toDF("Año","Avg_Temp") # reconvertimos a df
df_res.write.mode("overwrite").option("header", "true").option("sep",";").csv("Tavg8.csv") # guardamos en csv





