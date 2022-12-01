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
df.groupBy("Anio").agg({'Dia1':'avg', 'Dia2':'avg', 'Dia3':'avg', 'Dia4':'avg', 'Dia5':'avg', 'Dia6':'avg', 'Dia7':'avg', 'Dia8':'avg', 'Dia9':'avg', 'Dia10':'avg', 'Dia11':'avg', 'Dia12':'avg', 'Dia13':'avg', 'Dia14':'avg', 'Dia15':'avg', 'Dia16':'avg', 'Dia17':'avg', 'Dia18':'avg', 'Dia19':'avg', 'Dia20':'avg', 'Dia21':'avg', 'Dia22':'avg', 'Dia23':'avg', 'Dia24':'avg', 'Dia25':'avg', 'Dia26':'avg', 'Dia27':'avg', 'Dia28':'avg', 'Dia29':'avg', 'Dia30':'avg', 'Dia31':'avg'}).show()



DiasLluvia = []
DiasLluvia2 = []
def average(line): #para cada tupla año,mes calcula la media de temperatura
	sum = 0
	for i in range (3, 34):#no estan bien los rangos
		sum += line[i]
	return (line[0], sum/31)

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
t_max_avg_4.5 = df.rdd.map(average).collect() #rdd con tuplas (año,media de temperatura)
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
