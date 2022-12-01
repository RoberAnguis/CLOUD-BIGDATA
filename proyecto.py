#Tmax


from pyspark import SparkConf, SparkContext

from pyspark.sql import SparkSession

from pyspark.sql.functions import *
from decimal import Decimal

import sys
import re

conf = SparkConf().setAppName('ProyectoSpark')
sc = SparkContext(conf = conf)

spark = SparkSession.builder.appName('ProyectoSpark').getOrCreate()

df = spark.read.option("header", "true").option("sep",";").csv("datosTmax.csv")



#quitar los blancos más adelante

#Avg por año
df2 = df.groupBy("Año", "Mes").agg({'Día 1':'avg', 'Día 2':'avg', 'Día 3':'avg', 'Día 4':'avg', 'Día 5':'avg', 'Día 6':'avg', 'Día 7':'avg', 'Día 8':'avg', 'Día 9':'avg', 'Día 10':'avg', 'Día 11':'avg', 'Día 12':'avg', 'Día 13':'avg', 'Día 14':'avg', 'Día 15':'avg', 'Día 16':'avg', 'Día 17':'avg', 'Día 18':'avg', 'Día 19':'avg', 'Día 20':'avg', 'Día 21':'avg', 'Día 22':'avg', 'Día 23':'avg', 'Día 24':'avg', 'Día 25':'avg', 'Día 26':'avg', 'Día 27':'avg', 'Día 28':'avg', 'Día 29':'avg', 'Día 30':'avg', 'Día 31':'avg'}).sort("Año", "Mes").show()
#seguramente halla que poner groupBy("Año", "Mes") y luego especificar


def average(line): #para cada tupla año,mes calcula la media de temperatura
	sum = 0
	cont = 0
	for i in range (2, 33):#creo que estan bien los rangos
		if (line[i] != None):
			sum += Decimal(line[i])
			cont += 1
	return (line[0], (line[1], sum/cont))

			
	

#Hacer el avg de cada día
t_max_avg_4 = df2.rdd.map(average).reduceByKey(lambda a,b: ("basura", (a[1]+b[1]))).map(lambda x: (x[0], x[1][1]/12))
t_max_avg_4.collect()



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
