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
dfPre = spark.read.option("header", "true").option("sep",";").schema(schema).csv("Preci4.csv")


# Avg4 ------------------------------------------------------------------------------------------------------------------------------------------------------------------------

dfMax2 = dfMax.groupBy("Año", "Mes").agg({'Día 1':'avg', 'Día 2':'avg', 'Día 3':'avg', 'Día 4':'avg', 'Día 5':'avg', 'Día 6':'avg', 'Día 7':'avg', 'Día 8':'avg', 'Día 9':'avg', 'Día 10':'avg', 'Día 11':'avg', 'Día 12':'avg', 'Día 13':'avg', 'Día 14':'avg', 'Día 15':'avg', 'Día 16':'avg', 'Día 17':'avg', 'Día 18':'avg', 'Día 19':'avg', 'Día 20':'avg', 'Día 21':'avg', 'Día 22':'avg', 'Día 23':'avg', 'Día 24':'avg', 'Día 25':'avg', 'Día 26':'avg', 'Día 27':'avg', 'Día 28':'avg', 'Día 29':'avg', 'Día 30':'avg', 'Día 31':'avg'}).sort("Año", "Mes")
dfMin2 = dfMin.groupBy("Año", "Mes").agg({'Día 1':'avg', 'Día 2':'avg', 'Día 3':'avg', 'Día 4':'avg', 'Día 5':'avg', 'Día 6':'avg', 'Día 7':'avg', 'Día 8':'avg', 'Día 9':'avg', 'Día 10':'avg', 'Día 11':'avg', 'Día 12':'avg', 'Día 13':'avg', 'Día 14':'avg', 'Día 15':'avg', 'Día 16':'avg', 'Día 17':'avg', 'Día 18':'avg', 'Día 19':'avg', 'Día 20':'avg', 'Día 21':'avg', 'Día 22':'avg', 'Día 23':'avg', 'Día 24':'avg', 'Día 25':'avg', 'Día 26':'avg', 'Día 27':'avg', 'Día 28':'avg', 'Día 29':'avg', 'Día 30':'avg', 'Día 31':'avg'}).sort("Año", "Mes")

def average(line): #para cada tupla año,mes calcula la media de temperatura
	sum = 0
	cont = 0
	for i in range (2, 33):#creo que estan bien los rangos
		if (line[i] != None):
			sum += line[i]
			cont += 1
	return (line[0], (line[1], sum/cont))

#Hacer el avg de cada día
t_max_avg_4 = dfMax2.rdd.map(average).reduceByKey(lambda a,b: ("basura", (a[1]+b[1]))).map(lambda x: (x[0], x[1][1]/12))
t_max_avg_4.collect()

t_min_avg_4 = dfMin2.rdd.map(average).reduceByKey(lambda a,b: ("basura", (a[1]+b[1]))).map(lambda x: (x[0], x[1][1]/12))
t_min_avg_4.collect()


#hacer lo mismo con Tmin y hacer la media entre ambos

rdd_final1 = t_max_avg_4.join(t_min_avg_4)
rdd_final1.collect()

def mediaMinMax(line):
	media = 0
	media = (line[1][0] + line[1][1])/2
	return (line[0], media)

rdd_final1 = rdd_final1.map(mediaMinMax)
rdd_final1.collect()


df_res = spark.createDataFrame(rdd_final1).toDF("Año","Avg_Temp") # reconvertimos a df
df_res = df_res.sort("Año")
df_res.write.mode("overwrite").option("header", "true").option("sep",";").csv("Tavg4.csv") # guardamos en csv


dic = {}
dic2 = {}
contador = 0


dic = rdd_final1.collectAsMap()


for i in range (2007, 2101):
    dic2[str(i)] = dic[str(i)]-dic[str(i-1)]

lista = list(dic2.items())

df_res1 = spark.createDataFrame(lista, ["Año", "Variación"])
    

df_res1 = df_res1.sort("Año")
df_res1.write.mode("overwrite").option("header", "true").option("sep",";").csv("CambTemp4.csv") # guardamos en csv


# Desv4 -----------------------------------------------------------------------------------------------------------------------------------------------------------------------

# para cada tupla año, mes calcula la media de temperatura
def average2(line): 
	sum = 0
	cont = 0
	for i in range (2, 33):
		if (line[i] != None):
			sum += line[i]
			cont += 1
	return (line[0], (line[1], sum, cont))


# Hacer el avg de las temperaturas por año (reducimos meses y dias)
t_max_avg_4_2 = dfMax2.rdd.map(average2).reduceByKey(lambda a,b: ("basura", (a[1]+b[1]), (a[2]+b[2]))).map(lambda x: (x[0], x[1][1]/x[1][2]))
t_max_avg_4_2.collect()

t_min_avg_4_2 = dfMin2.rdd.map(average2).reduceByKey(lambda a,b: ("basura", (a[1]+b[1]), (a[2]+b[2]))).map(lambda x: (x[0], x[1][1]/x[1][2]))
t_min_avg_4_2.collect()

# hacer la media entre ambos
rdd_mix1 = t_max_avg_4_2.join(t_min_avg_4_2)
rdd_avg = rdd_mix1.map(lambda line: (line[0], (line[1][0] + line[1][1])/2))


# pasamos a calcular las medias por día en vez de por año
df_mix2 = dfMax2.join(dfMin2, [dfMax2[0] == dfMin[0], dfMax2[1] == dfMin[1]])

def mediaMinMax2(line):
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
rdd_sumsq = df_mix2.rdd.map(mediaMinMax2).map(sumSq).reduceByKey(lambda a,b: ("basura", (a[1]+b[1]), (a[2]+b[2]))).map(lambda x: (x[0], x[1][1]/x[1][2]))

# calculamos el resultado de sumatorio de los xi²/n - la media anual ^2 = varianza, de la que sacamos la desviacion típica con una raiz cuadrada
rdd_mix3 = rdd_sumsq.join(rdd_avg)
rdd_res2 = rdd_mix3.map(lambda a: (a[0], math.sqrt(a[1][0] - (a[1][1]**2))))


df_res2 = spark.createDataFrame(rdd_res2).toDF("Año", "Desviacion_tipica") # reconvertimos a df
df_res2 = df_res2.sort("Año")
df_res2.write.mode("overwrite").option("header", "true").option("sep",";").csv("Desv4_results.csv") # guardamos en csv



# Tmax ----------------------------------------------------------------------------------------------------------------------------------------------------------------------

# Maximo por cada día del mes (reducimos una fila)
dfMax3 = dfMax2.groupBy("Año").max('avg(Día 1)', 'avg(Día 2)', 'avg(Día 3)', 'avg(Día 4)', 'avg(Día 5)', 'avg(Día 6)', 'avg(Día 7)', \
	'avg(Día 8)', 'avg(Día 9)', 'avg(Día 10)', 'avg(Día 11)', 'avg(Día 12)', 'avg(Día 13)', 'avg(Día 14)', 'avg(Día 15)', 'avg(Día 16)', \
	'avg(Día 17)', 'avg(Día 18)', 'avg(Día 19)', 'avg(Día 20)', 'avg(Día 21)', 'avg(Día 22)', 'avg(Día 23)', 'avg(Día 24)', \
	'avg(Día 25)', 'avg(Día 26)', 'avg(Día 27)', 'avg(Día 28)', 'avg(Día 29)', 'avg(Día 30)', 'avg(Día 31)').sort("Año")

# para cada año calcula la maxima temperatura (reducimos columnas)
def max_month(line): 
	maxi = 0
	for i in range (1, 32):
		if (line[i] != None and line[i] > maxi): maxi = line[i]
	return (line[0], maxi)


t_max_4_max = dfMax3.rdd.map(max_month) 
df_res3 = spark.createDataFrame(t_max_4_max).toDF("Año","Temp_Max") # reconvertimos a df
df_res3.write.mode("overwrite").option("header", "true").option("sep",";").csv("Tmax4_result.csv") # guardamos en csv



# Tmin ----------------------------------------------------------------------------------------------------------------------------------------------------------------------

# Minimo por cada día del mes (reducimos una fila)
dfMin3 = dfMin2.groupBy("Año").min('avg(Día 1)', 'avg(Día 2)', 'avg(Día 3)', 'avg(Día 4)', 'avg(Día 5)', 'avg(Día 6)', 'avg(Día 7)', \
	'avg(Día 8)', 'avg(Día 9)', 'avg(Día 10)', 'avg(Día 11)', 'avg(Día 12)', 'avg(Día 13)', 'avg(Día 14)', 'avg(Día 15)', 'avg(Día 16)', \
	'avg(Día 17)', 'avg(Día 18)', 'avg(Día 19)', 'avg(Día 20)', 'avg(Día 21)', 'avg(Día 22)', 'avg(Día 23)', 'avg(Día 24)', \
	'avg(Día 25)', 'avg(Día 26)', 'avg(Día 27)', 'avg(Día 28)', 'avg(Día 29)', 'avg(Día 30)', 'avg(Día 31)').sort("Año")

# para cada año calcula la minima temperatura (reducimos columnas)
def min_month(line): 
	min = 50 # mas alto que cualquier temperatura del csv
	for i in range (1, 32):
		if (line[i] != None and line[i] < min): min = line[i]
	return (line[0], min)


t_min_4_min = dfMin3.rdd.map(min_month) 
df_res4 = spark.createDataFrame(t_min_4_min).toDF("Año","Temp_Min") # reconvertimos a df
df_res4.write.mode("overwrite").option("header", "true").option("sep",";").csv("Tmin4_results.csv") # guardamos en csv



# diasLluvia ------------------------------------------------------------------------------------------------------------------------------------------------------------

def diasLluvia(line): 
	cont = 0
	for i in range (6, 37):
		if (line[i] != None):
			if(line[i] > 0):
				cont += 1
	return (line[0], line[1], line[2], line[3], cont)

#Hacer el avg de cada día
dias_lluvia_4 = dfPre.rdd.map(diasLluvia)
df_res5 = spark.createDataFrame(dias_lluvia_4).toDF("Año","Mes","Modelo","Punto", "DiasLluvia")
df2 = df_res5.groupBy("Año", "Mes", "Modelo").avg("DiasLluvia")
df3 = df2.groupBy("Año", "Mes").avg("avg(DiasLluvia)")
df_final5 = df3.groupBy("Año").sum("avg(avg(DiasLluvia))").sort("Año")
df_fin = df_final5.withColumnRenamed("sum(avg(avg(DiasLluvia)))","nDiasLluvia")

df_fin.write.mode("overwrite").option("header", "true").option("sep",";").csv("DiasLluvia4.csv") # guardamos en csv




# Preci4 -------------------------------------------------------------------------------------------------------------------------------------------------------------------

#Media diaria 
df_day = dfPre.groupBy("Año","Mes").avg('Día 1','Día 2','Día 3','Día 4','Día 5','Día 6','Día 7','Día 8','Día 9','Día 10','Día 11','Día 12','Día 13','Día 14','Día 15','Día 16','Día 17','Día 18','Día 19','Día 20','Día 21','Día 22','Día 23','Día 24','Día 25','Día 26','Día 27','Día 28','Día 29','Día 30','Día 31').sort('Año', 'Mes')

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
df_final6 = spark.createDataFrame(avg_preci).toDF("Año","Avg_Preci")
df_final6.write.mode("overwrite").option("header", "true").option("sep",";").csv("AvgPreci_4.csv")


######################################################################################################################################################################

######################################################################################################################################################################

# Escenario 8

# cargamos los datos
dfMax = spark.read.option("header", "true").option("sep",";").schema(schema).csv("Tmax8.csv")
dfMin = spark.read.option("header", "true").option("sep",";").schema(schema).csv("Tmin8.csv")
dfPre = spark.read.option("header", "true").option("sep",";").schema(schema).csv("Preci8.csv")


# Avg4 ------------------------------------------------------------------------------------------------------------------------------------------------------------------------

dfMax2 = dfMax.groupBy("Año", "Mes").agg({'Día 1':'avg', 'Día 2':'avg', 'Día 3':'avg', 'Día 4':'avg', 'Día 5':'avg', 'Día 6':'avg', 'Día 7':'avg', 'Día 8':'avg', 'Día 9':'avg', 'Día 10':'avg', 'Día 11':'avg', 'Día 12':'avg', 'Día 13':'avg', 'Día 14':'avg', 'Día 15':'avg', 'Día 16':'avg', 'Día 17':'avg', 'Día 18':'avg', 'Día 19':'avg', 'Día 20':'avg', 'Día 21':'avg', 'Día 22':'avg', 'Día 23':'avg', 'Día 24':'avg', 'Día 25':'avg', 'Día 26':'avg', 'Día 27':'avg', 'Día 28':'avg', 'Día 29':'avg', 'Día 30':'avg', 'Día 31':'avg'}).sort("Año", "Mes")
dfMin2 = dfMin.groupBy("Año", "Mes").agg({'Día 1':'avg', 'Día 2':'avg', 'Día 3':'avg', 'Día 4':'avg', 'Día 5':'avg', 'Día 6':'avg', 'Día 7':'avg', 'Día 8':'avg', 'Día 9':'avg', 'Día 10':'avg', 'Día 11':'avg', 'Día 12':'avg', 'Día 13':'avg', 'Día 14':'avg', 'Día 15':'avg', 'Día 16':'avg', 'Día 17':'avg', 'Día 18':'avg', 'Día 19':'avg', 'Día 20':'avg', 'Día 21':'avg', 'Día 22':'avg', 'Día 23':'avg', 'Día 24':'avg', 'Día 25':'avg', 'Día 26':'avg', 'Día 27':'avg', 'Día 28':'avg', 'Día 29':'avg', 'Día 30':'avg', 'Día 31':'avg'}).sort("Año", "Mes")

def average(line): #para cada tupla año,mes calcula la media de temperatura
	sum = 0
	cont = 0
	for i in range (2, 33):#creo que estan bien los rangos
		if (line[i] != None):
			sum += line[i]
			cont += 1
	return (line[0], (line[1], sum/cont))

#Hacer el avg de cada día
t_max_avg_8 = dfMax2.rdd.map(average).reduceByKey(lambda a,b: ("basura", (a[1]+b[1]))).map(lambda x: (x[0], x[1][1]/12))
t_max_avg_8.collect()

t_min_avg_8 = dfMin2.rdd.map(average).reduceByKey(lambda a,b: ("basura", (a[1]+b[1]))).map(lambda x: (x[0], x[1][1]/12))
t_min_avg_8.collect()


#hacer lo mismo con Tmin y hacer la media entre ambos

rdd_final1 = t_max_avg_8.join(t_min_avg_8)
rdd_final1.collect()

rdd_final1 = rdd_final1.map(mediaMinMax)
rdd_final1.collect()


df_res = spark.createDataFrame(rdd_final1).toDF("Año","Avg_Temp") # reconvertimos a df
df_res = df_res.sort("Año")
df_res.write.mode("overwrite").option("header", "true").option("sep",";").csv("Tavg8.csv") # guardamos en csv


dic = {}
dic2 = {}
contador = 0


dic = rdd_final1.collectAsMap()


for i in range (2007, 2101):
    dic2[str(i)] = dic[str(i)]-dic[str(i-1)]

lista = list(dic2.items())

df_res1 = spark.createDataFrame(lista, ["Año", "Variación"])
    
df_res1 = df_res1.sort("Año")
df_res1.write.mode("overwrite").option("header", "true").option("sep",";").csv("CambTemp8.csv") # guardamos en csv


# Desv4 -----------------------------------------------------------------------------------------------------------------------------------------------------------------------

# Hacer el avg de las temperaturas por año (reducimos meses y dias)
t_max_avg_8_2 = dfMax2.rdd.map(average2).reduceByKey(lambda a,b: ("basura", (a[1]+b[1]), (a[2]+b[2]))).map(lambda x: (x[0], x[1][1]/x[1][2]))
t_max_avg_8_2.collect()

t_min_avg_8_2 = dfMin2.rdd.map(average2).reduceByKey(lambda a,b: ("basura", (a[1]+b[1]), (a[2]+b[2]))).map(lambda x: (x[0], x[1][1]/x[1][2]))
t_min_avg_8_2.collect()

# hacer la media entre ambos
rdd_mix1 = t_max_avg_8_2.join(t_min_avg_8_2)
rdd_avg = rdd_mix1.map(lambda line: (line[0], (line[1][0] + line[1][1])/2))


# pasamos a calcular las medias por día en vez de por año
df_mix2 = dfMax2.join(dfMin2, [dfMax2[0] == dfMin[0], dfMax2[1] == dfMin[1]])

# calculamos las medias por día xi y calculamos el sumatorio de todos los n xi², que luego dividimos entre n
rdd_sumsq = df_mix2.rdd.map(mediaMinMax2).map(sumSq).reduceByKey(lambda a,b: ("basura", (a[1]+b[1]), (a[2]+b[2]))).map(lambda x: (x[0], x[1][1]/x[1][2]))

# calculamos el resultado de sumatorio de los xi²/n - la media anual ^2 = varianza, de la que sacamos la desviacion típica con una raiz cuadrada
rdd_mix3 = rdd_sumsq.join(rdd_avg)
rdd_res2 = rdd_mix3.map(lambda a: (a[0], math.sqrt(a[1][0] - (a[1][1]**2))))


df_res2 = spark.createDataFrame(rdd_res2).toDF("Año", "Desviacion_tipica") # reconvertimos a df
df_res2 = df_res2.sort("Año")
df_res2.write.mode("overwrite").option("header", "true").option("sep",";").csv("Desv8_results.csv") # guardamos en csv



# Tmax ----------------------------------------------------------------------------------------------------------------------------------------------------------------------

# Maximo por cada día del mes (reducimos una fila)
dfMax3 = dfMax2.groupBy("Año").max('avg(Día 1)', 'avg(Día 2)', 'avg(Día 3)', 'avg(Día 4)', 'avg(Día 5)', 'avg(Día 6)', 'avg(Día 7)', \
	'avg(Día 8)', 'avg(Día 9)', 'avg(Día 10)', 'avg(Día 11)', 'avg(Día 12)', 'avg(Día 13)', 'avg(Día 14)', 'avg(Día 15)', 'avg(Día 16)', \
	'avg(Día 17)', 'avg(Día 18)', 'avg(Día 19)', 'avg(Día 20)', 'avg(Día 21)', 'avg(Día 22)', 'avg(Día 23)', 'avg(Día 24)', \
	'avg(Día 25)', 'avg(Día 26)', 'avg(Día 27)', 'avg(Día 28)', 'avg(Día 29)', 'avg(Día 30)', 'avg(Día 31)').sort("Año")


t_max_8_max = dfMax3.rdd.map(max_month) 
df_res3 = spark.createDataFrame(t_max_8_max).toDF("Año","Temp_Max") # reconvertimos a df
df_res3.write.mode("overwrite").option("header", "true").option("sep",";").csv("Tmax8_result.csv") # guardamos en csv



# Tmin ----------------------------------------------------------------------------------------------------------------------------------------------------------------------

# Minimo por cada día del mes (reducimos una fila)
dfMin3 = dfMin2.groupBy("Año").min('avg(Día 1)', 'avg(Día 2)', 'avg(Día 3)', 'avg(Día 4)', 'avg(Día 5)', 'avg(Día 6)', 'avg(Día 7)', \
	'avg(Día 8)', 'avg(Día 9)', 'avg(Día 10)', 'avg(Día 11)', 'avg(Día 12)', 'avg(Día 13)', 'avg(Día 14)', 'avg(Día 15)', 'avg(Día 16)', \
	'avg(Día 17)', 'avg(Día 18)', 'avg(Día 19)', 'avg(Día 20)', 'avg(Día 21)', 'avg(Día 22)', 'avg(Día 23)', 'avg(Día 24)', \
	'avg(Día 25)', 'avg(Día 26)', 'avg(Día 27)', 'avg(Día 28)', 'avg(Día 29)', 'avg(Día 30)', 'avg(Día 31)').sort("Año")


t_min_8_min = dfMin3.rdd.map(min_month) 
df_res4 = spark.createDataFrame(t_min_8_min).toDF("Año","Temp_Min") # reconvertimos a df
df_res4.write.mode("overwrite").option("header", "true").option("sep",";").csv("Tmin8_results.csv") # guardamos en csv



# diasLluvia ------------------------------------------------------------------------------------------------------------------------------------------------------------

#Hacer el avg de cada día
dias_lluvia_8 = dfPre.rdd.map(diasLluvia)
df_res5 = spark.createDataFrame(dias_lluvia_8).toDF("Año","Mes","Modelo","Punto", "DiasLluvia")
df2 = df_res5.groupBy("Año", "Mes", "Modelo").avg("DiasLluvia")
df3 = df2.groupBy("Año", "Mes").avg("avg(DiasLluvia)")
df_final5 = df3.groupBy("Año").sum("avg(avg(DiasLluvia))").sort("Año")
df_fin = df_final5.withColumnRenamed("sum(avg(avg(DiasLluvia)))","nDiasLluvia")

df_fin.write.mode("overwrite").option("header", "true").option("sep",";").csv("DiasLluvia8.csv") # guardamos en csv


# Preci4 -------------------------------------------------------------------------------------------------------------------------------------------------------------------

#Media diaria 
df_day = dfPre.groupBy("Año","Mes").avg('Día 1','Día 2','Día 3','Día 4','Día 5','Día 6','Día 7','Día 8','Día 9','Día 10','Día 11','Día 12','Día 13','Día 14','Día 15','Día 16','Día 17','Día 18','Día 19','Día 20','Día 21','Día 22','Día 23','Día 24','Día 25','Día 26','Día 27','Día 28','Día 29','Día 30','Día 31').sort('Año', 'Mes')

#Reconversión a df y guardado en un csv
avg_preci = df_day.rdd.map(avg_preci_year).reduceByKey(lambda a,b: ("basura", (a[1]+b[1]), (a[2]+b[2]))).map(lambda x: (x[0], x[1][1]/x[1][2]))
df_final6 = spark.createDataFrame(avg_preci).toDF("Año","Avg_Preci")
df_final6.write.mode("overwrite").option("header", "true").option("sep",";").csv("AvgPreci_8.csv")