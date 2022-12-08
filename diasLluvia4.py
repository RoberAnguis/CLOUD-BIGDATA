
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

df = spark.read.option("header", "true").option("sep",";").schema(schema).csv("Preci4.csv")


def diasLluvia(line): #para cada tupla año,mes calcula la media de temperatura
	cont = 0
	for i in range (6, 37):#creo que estan bien los rangos
		if (line[i] != None):
			if(line[i] > 0):
				cont += 1
	return (line[0], line[1], line[2], line[3], cont)

			
	

#Hacer el avg de cada día
dias_lluvia_4 = df.rdd.map(diasLluvia)
df_res = spark.createDataFrame(dias_lluvia_4).toDF("Año","Mes","Modelo","Punto", "DiasLluvia")
df2 = df_res.groupBy("Año", "Mes", "Modelo").avg("DiasLluvia")
df3 = df2.groupBy("Año", "Mes").avg("avg(DiasLluvia)")
df_final = df3.groupBy("Año").sum("avg(avg(DiasLluvia))").sort("Año")

df_fin = df_final.withColumnRenamed("sum(avg(avg(DiasLluvia)))","nDiasLluvia")



df_fin.write.mode("overwrite").option("header", "true").option("sep",";").csv("DiasLluvia4.csv") # guardamos en csv