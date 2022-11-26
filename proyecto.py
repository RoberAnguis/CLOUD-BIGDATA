#Tmax


from pyspark import SparkConf, SparkContext

from pyspark.sql import SparkSession

from pyspark.sql.functions import *


import sys
import re

conf = SparkConf().setAppName('ProyectoSpark')
sc = SparkContext(conf = conf)

spark = SparkSession.builder.appName('ProyectoSpark').getOrCreate()

df = spark.read.option("header", "true").csv("datosTmax.csv")

#quitar los blancos más adelante

#Avg por año, mes y modelo
df.groupBy("Anio", "Mes", "Modelo").agg({'Dia1':'avg', 'Dia2':'avg', 'Dia3':'avg', 'Dia4':'avg', 'Dia5':'avg', 'Dia6':'avg', 'Dia7':'avg', 'Dia8':'avg', 'Dia9':'avg', 'Dia10':'avg', 'Dia11':'avg', 'Dia12':'avg', 'Dia13':'avg', 'Dia14':'avg', 'Dia15':'avg', 'Dia16':'avg', 'Dia17':'avg', 'Dia18':'avg', 'Dia19':'avg', 'Dia20':'avg', 'Dia21':'avg', 'Dia22':'avg', 'Dia23':'avg', 'Dia24':'avg', 'Dia25':'avg', 'Dia26':'avg', 'Dia27':'avg', 'Dia28':'avg', 'Dia29':'avg', 'Dia30':'avg', 'Dia31':'avg'}).show()



def average(line): #para cada tupla año,mes,modelo calcula la media de temperatura
	sum = 0
	for i in range (3, 34):#no se si estan bien los rangos
		sum += line[i]
	return (line[0], line[1], line[2], sum/31)


#Hacer el avg de cada día
rddAux = df.rdd.map(average). collect() #rdd con tuplas año,mes,modelo,media de temperatura(en ese mes de ese año y con ese modelo)

''' Ejemplo:

Año	Mes 	Modelo		Avg

2006     1        1.0            34
2006     2        1.0            14
.
.
.
2006    1         2.0           23
.
.
.
2100    12        17.0          27
 '''

#De aqui podemos sacar mas facilmente ya media por años o modelos, segun nos apetezca