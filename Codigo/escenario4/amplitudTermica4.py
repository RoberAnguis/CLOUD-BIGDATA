from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType,StructField, StringType, DoubleType, IntegerType
from decimal import Decimal

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
    
df = spark.read.option("header", "true").option("sep",";").schema(schema).csv("AmpT4.csv")
df1 = df.groupBy("Año", "Mes").agg({'Día 1':'avg', 'Día 2':'avg', 'Día 3':'avg', 'Día 4':'avg', 'Día 5':'avg', 'Día 6':'avg', 'Día 7':'avg', 'Día 8':'avg', 'Día 9':'avg', 'Día 10':'avg', 'Día 11':'avg', 'Día 12':'avg', 'Día 13':'avg', 'Día 14':'avg', 'Día 15':'avg', 'Día 16':'avg', 'Día 17':'avg', 'Día 18':'avg', 'Día 19':'avg', 'Día 20':'avg', 'Día 21':'avg', 'Día 22':'avg', 'Día 23':'avg', 'Día 24':'avg', 'Día 25':'avg', 'Día 26':'avg', 'Día 27':'avg', 'Día 28':'avg', 'Día 29':'avg', 'Día 30':'avg', 'Día 31':'avg'}).sort("Año", "Mes")
df2 = df1.groupBy("Año").agg({'avg(Día 1)':'max', 'avg(Día 2)':'max', 'avg(Día 3)':'max', 'avg(Día 4)':'max', 'avg(Día 5)':'max', 'avg(Día 6)':'max', 'avg(Día 7)':'max', 'avg(Día 8)':'max', 'avg(Día 9)':'max', 'avg(Día 10)':'max', 'avg(Día 11)':'max', 'avg(Día 12)':'max', 'avg(Día 13)':'max', 'avg(Día 14)':'max', 'avg(Día 15)':'max', 'avg(Día 16)':'max', 'avg(Día 17)':'max', 'avg(Día 18)':'max', 'avg(Día 19)':'max', 'avg(Día 20)':'max', 'avg(Día 21)':'max', 'avg(Día 22)':'max', 'avg(Día 23)':'max', 'avg(Día 24)':'max', 'avg(Día 25)':'max', 'avg(Día 26)':'max', 'avg(Día 27)':'max', 'avg(Día 28)':'max', 'avg(Día 29)':'max', 'avg(Día 30)':'max', 'avg(Día 31)':'max'}).sort('Año')

def amplitude(line): #calculamos el maximo de la amplitud termica de un año
	maximo = 0
	for i in range (1, 32):
		if(line[i]!=None):
			if(line[i]>maximo):
				maximo = line[i]
	return (line[0], maximo)

result = df2.rdd.map(amplitude)
dfResult = spark.createDataFrame(result, ["Año", "Maxima Amplitud"])
dfResult.write.mode("overwrite").option("header", "true").option("sep",";").csv("AmpTerm4.csv") # guardamos en csv

