#Tmax


from pyspark import SparkConf, SparkContext

from pyspark.sql import SparkSession

from pyspark.sql.functions import *


import sys
import re

conf = SparkConf().setAppName('ProyectoSpark')
sc = SparkContext(conf = conf)

spark = SparkSession.builder.appName('ProyectoSpark').getOrCreate()

df = spark.read.option("header", "true").option("sep",";").csv("datosTmax.csv")

precip_4 = spark.read.options("header", "true").option("sep",";").csv("datosPrecip4.csv")

#quitar los blancos más adelante

#Avg por año
df.groupBy("Año").agg({'Día 1':'avg', 'Día 2':'avg', 'Día 3':'avg', 'Día 4':'avg', 'Día 5':'avg', 'Día 6':'avg', 'Día 7':'avg', 'Día 8':'avg', 'Día 9':'avg', 'Día 10':'avg', 'Día 11':'avg', 'Día 12':'avg', 'Día 13':'avg', 'Día 14':'avg', 'Día 15':'avg', 'Día 16':'avg', 'Día 17':'avg', 'Día 18':'avg', 'Día 19':'avg', 'Día 20':'avg', 'Día 21':'avg', 'Día 22':'avg', 'Día 23':'avg', 'Día 24':'avg', 'Día 25':'avg', 'Día 26':'avg', 'Día 27':'avg', 'Día 28':'avg', 'Día 29':'avg', 'Día 30':'avg', 'Día 31':'avg'}).sort("Año").show()
#seguramente halla que poner groupBy("Año", "Mes") y luego especificar


DiasLluvia = []
DiasLluvia2 = []
def average(line): #para cada tupla año,mes calcula la media de temperatura
	sum = 0
	for i in range (2, 33):#creo que estan bien los rangos
		sum += line[i]
	return (line[0], (line[1], sum/31))

def nDiasLluvia(line):
	cont = 0
	for i in range (6, 37):#no estan bien los rangos
		if(line[i] > 0):
			cont += 1
	if((line[0], line[1]) in DiasLluvia):
		return ((-1,-1), -1) #basura para poder filtrarla despues
	else:
		DiasLluvia[(line[0],line[1])] = cont
	return ((line[0], line[1]), cont) #n de dias con lluvia en año, mes (voy a coger para cada año/mes uno arbitrario de entre todas la rejillas y modelos)

def nLluviaAnio(line):#suma para cada año los dias de lluvia, deja los resultados en el diccionario DiasLluvia2
	if (line[0][0] == -1):
		return
	else:
		if(line[0][0] in DiasLluvia2):
			valor = DiasLluvia2[line[0][0]]
			valor += line[1]
			DiasLluvia2[line[0][0]] = valor
		else:
			DiasLluvia2[line[0][0]] = line[1]
		return 
			
	

#Hacer el avg de cada día
t_max_avg_4.5 = df.rdd.map(average).collect() #rdd con tuplas (año,media de temperatura) aqui hay que hacer un reduce by key
n_dias_precip_4.5 = precip_4.rdd.map(nDiasLluvia).map(nLluviaAnio).collect() #deja los resultados en el diccionario DiasLluvia2




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
