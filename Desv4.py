

from pyspark import SparkConf, SparkContext

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, DoubleType
from pyspark.sql.functions import *
import math


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

# cargamos los datos
dfMax = spark.read.option("header", "true").option("sep",";").schema(schema).csv("Tmax4.csv")
dfMin = spark.read.option("header", "true").option("sep",";").schema(schema).csv("Tmin4.csv")

# Avg por año, mes y día (reducimos los modelos y los puntos)
dfMax2 = dfMax.groupBy("Año", "Mes").agg({'Día 1':'avg', 'Día 2':'avg', 'Día 3':'avg', 'Día 4':'avg', 'Día 5':'avg', 'Día 6':'avg', 'Día 7':'avg', 'Día 8':'avg', 'Día 9':'avg', 'Día 10':'avg', 'Día 11':'avg', 'Día 12':'avg', 'Día 13':'avg', 'Día 14':'avg', 'Día 15':'avg', 'Día 16':'avg', 'Día 17':'avg', 'Día 18':'avg', 'Día 19':'avg', 'Día 20':'avg', 'Día 21':'avg', 'Día 22':'avg', 'Día 23':'avg', 'Día 24':'avg', 'Día 25':'avg', 'Día 26':'avg', 'Día 27':'avg', 'Día 28':'avg', 'Día 29':'avg', 'Día 30':'avg', 'Día 31':'avg'}).sort("Año", "Mes")
dfMin2 = dfMin.groupBy("Año", "Mes").agg({'Día 1':'avg', 'Día 2':'avg', 'Día 3':'avg', 'Día 4':'avg', 'Día 5':'avg', 'Día 6':'avg', 'Día 7':'avg', 'Día 8':'avg', 'Día 9':'avg', 'Día 10':'avg', 'Día 11':'avg', 'Día 12':'avg', 'Día 13':'avg', 'Día 14':'avg', 'Día 15':'avg', 'Día 16':'avg', 'Día 17':'avg', 'Día 18':'avg', 'Día 19':'avg', 'Día 20':'avg', 'Día 21':'avg', 'Día 22':'avg', 'Día 23':'avg', 'Día 24':'avg', 'Día 25':'avg', 'Día 26':'avg', 'Día 27':'avg', 'Día 28':'avg', 'Día 29':'avg', 'Día 30':'avg', 'Día 31':'avg'}).sort("Año", "Mes")

# para cada tupla año, mes calcula la media de temperatura
def average(line): 
	sum = 0
	cont = 0
	for i in range (2, 33):
		if (line[i] != None):
			sum += line[i]
			cont += 1
	return (line[0], (line[1], sum, cont))


# Hacer el avg de las temperaturas por año (reducimos meses y dias)
t_max_avg_4 = dfMax2.rdd.map(average).reduceByKey(lambda a,b: ("basura", (a[1]+b[1]), (a[2]+b[2]))).map(lambda x: (x[0], x[1][1]/x[1][2]))
t_max_avg_4.collect()

t_min_avg_4 = dfMin2.rdd.map(average).reduceByKey(lambda a,b: ("basura", (a[1]+b[1]), (a[2]+b[2]))).map(lambda x: (x[0], x[1][1]/x[1][2]))
t_min_avg_4.collect()

# hacer la media entre ambos
rdd_mix1 = t_max_avg_4.join(t_min_avg_4)
rdd_avg = rdd_mix1.map(lambda line: (line[0], (line[1][0] + line[1][1])/2))


# pasamos a calcular las medias por día en vez de por año
df_mix2 = dfMax2.join(dfMin2, [dfMax2[0] == dfMin[0], dfMax2[1] == dfMin[1]])

def mediaMinMax(line):
	media = list()
	for i in range (2, 33):
		if (line[i] != None):
			media.append((line[i] + line[i + 33]) / 2)
	return (line[0], line[1], media)

def sumSq(line):
	sumsq = 0
	count = len(line[2])
	for i in range (0, count):
		if (line[2][i] != None):
			sumsq += line[2][i]**2
	return (line[0], (line[1], sumsq, count))

# calculamos las medias por día xi y calculamos el sumatorio de todos los n xi², que luego dividimos entre n
rdd_sumsq = df_mix2.rdd.map(mediaMinMax).map(sumSq).reduceByKey(lambda a,b: ("basura", (a[1]+b[1]), (a[2]+b[2]))).map(lambda x: (x[0], x[1][1]/x[1][2]))

# calculamos el resultado de sumatorio de los xi²/n - la media anual ^2 = varianza, de la que sacamos la desviacion típica con una raiz cuadrada
rdd_mix3 = rdd_sumsq.join(rdd_avg)
rdd_res = rdd_mix3.map(lambda a: (a[0], math.sqrt(a[1][0] - (a[1][1]**2))))


df_res = spark.createDataFrame(rdd_res).toDF("Año", "Desviacion_tipica") # reconvertimos a df
df_res = df_res.sort("Año")
df_res.write.mode("overwrite").option("header", "true").option("sep",";").csv("Desv4_results.csv") # guardamos en csv







