# Climate predictions in Madrid

## Descripción.

Nuestro proyecto trata sobre la interpretación y el análisis de datos proporcionados por dos escenarios posibles, que hacen referencia a las emisiones de gases de efecto invernadero, durante el próximo siglo.

Durante este proyecto analizaremos los datos obtenidos de la web Adapteca, con referencia a la Comunidad de Madrid, donde diferenciaremos dos posibles escenarios; el RCP 4.5 y el RCP 8.5.

El escenario RCP 4.5 es un escenario realista durante el cual las emisiones de dichos gases comienzan a ser controladas en la década de los cuarenta,  esto como veremos en el análisis producirán cambios durante la primera mitad del siglo que se estabilizarán durante la segunda mitad del mismo.

Por otro lado el escenario RCP 8.5, es un escenario más alarmista en el cual estas emisiones no son controladas, lo cuál producirá mayores cambios en todas las mediciones; pese a esto no es un escenario descartable, ya que es al que nos dirigimos en caso de no tomar medidas.

## Explicación del contenido.

Este Github se divide en los siguientes seis apartados:

* Carpeta escenario 4:  encontraremos en su interior los datos iniciales utilizados como input en cada uno de los códigos ejecutados del escenario 4.
* Carpeta escenario 8: encontraremos en su interior los datos iniciales utilizados como input en cada uno de los códigos ejecutados del escenario 8.
* Carpeta CSV: dentro de esta carpeta encontraremos los resultados de cada uno de los ejercicios ejecutados por separado, agrupándolos por escenarios.
* Carpeta Código: en esta carpeta nos encontraremos con los diferentes códigos agrupados por escenarios, además de uno en el que se encuentra todo el código.
* Carpeta Gráficas: en esta carpeta hallaremos las diferentes gráficas producidas a partir de los datos obtenidos, usadas tanto en la página web como en la presentación final.
* Fichero Wordpress: Este fichero contiene un link a la página del proyecto.


## Ejecución del código.

Para ejecutar cualquier fichero es necesario tener instalado Python y Pyspark. Para Pyspark basta con ejecutar:

pip install pyspark

O seguir las instrucciones en este enlace: https://spark.apache.org/docs/latest/api/python/getting_started/install.html

Para ejecutar un fichero, es necesario que se encuentre en la misma carpeta que los datos, el formato es el siguiente:

spark-submit <file_name>

Los resultados aparecerán en el mismo directorio  dentro de una carpeta con el nombre apropiado que contendrá el csv de salida.

Para ejecutar el archivo Todo.py que realiza todos los cálculos de una sola vez, es necesario seguir un formato distinto:

spark-submit Todo.py <input_folder> <output_folder>

Dónde input_folder es la carpeta que contiene los ficheros csv de entrada y output_folder el nombre del directorio donde se guardarán los csv resultado.

## Enlace a la web.
https://proyectocloud2022.wordpress.com/
