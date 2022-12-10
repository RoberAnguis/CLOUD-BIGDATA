#Tmin

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType,StructField, StringType, DoubleType


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
    
     
df = spark.read.option("header", "true").option("sep",";").schema(schema).csv("Tmin4.csv")

# Media entre los diferentes puntos y modelos para cada dia de cada mes de cada año (reducimos las filas)
df2 = df.groupBy("Año", "Mes").avg('Día 1', 'Día 2', 'Día 3', 'Día 4', 'Día 5', 'Día 6', 'Día 7', 'Día 8', 'Día 9', \
 'Día 10', 'Día 11', 'Día 12', 'Día 13', 'Día 14', 'Día 15', 'Día 16', 'Día 17', 'Día 18', 'Día 19', 'Día 20', \
 'Día 21', 'Día 22', 'Día 23', 'Día 24', 'Día 25', 'Día 26', 'Día 27', 'Día 28', 'Día 29', 'Día 30', 'Día 31')

# Minimo por cada día del mes (reducimos una fila)
df3 = df2.groupBy("Año").min('avg(Día 1)', 'avg(Día 2)', 'avg(Día 3)', 'avg(Día 4)', 'avg(Día 5)', 'avg(Día 6)', 'avg(Día 7)', \
	'avg(Día 8)', 'avg(Día 9)', 'avg(Día 10)', 'avg(Día 11)', 'avg(Día 12)', 'avg(Día 13)', 'avg(Día 14)', 'avg(Día 15)', 'avg(Día 16)', \
	'avg(Día 17)', 'avg(Día 18)', 'avg(Día 19)', 'avg(Día 20)', 'avg(Día 21)', 'avg(Día 22)', 'avg(Día 23)', 'avg(Día 24)', \
	'avg(Día 25)', 'avg(Día 26)', 'avg(Día 27)', 'avg(Día 28)', 'avg(Día 29)', 'avg(Día 30)', 'avg(Día 31)').sort("Año")

# para cada año calcula la minima temperatura (reducimos columnas)
def min_month(line): 
	min = 50 # mas alto que cualquier temperatura del csv
	for i in range (1, 32):
		if (line[i] != None and line[i] < min): min = line[i]
	return (line[0], min)



t_min_4_min = df3.rdd.map(min_month) 
df_res = spark.createDataFrame(t_min_4_min).toDF("Año","Temp_Min") # reconvertimos a df
df_res.write.mode("overwrite").option("header", "true").option("sep",";").csv("Tmin4_results.csv") # guardamos en csv
