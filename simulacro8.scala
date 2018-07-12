// 1. Leer el fichero y seleccionar las columnas(dia de la semana, distrito, lugar del accidente, nº victimas, tipo de accidente, tipo de persona, sexo, tramo de edad).
//Guardar en avro + snappy
//primero leemos el fichero, y vamos a quitarle la cabecera
val lectura = sc.textFile("/user/cert/simulacros/simulacro8/accidentes")
val cabecera = lectura.first
val body = lectura.filter( x => x != cabecera).map(_.split(';'))
//hemos spliteado a la vez que quitamos la cabecera, procedemos a quedarnos los campos que nos interesan
//como ciertos campos tienen muchos espacios le hacemos .trim a esos campos para eliminar esos espacios que sobran
val camposutiles = body.map(x => (x(2), x(3).trim, x(4).trim, x(19).toInt, x(20).trim, x(22).trim, x(23), x(25)))
//para poder guardarlos en avro tenemos que darle una estructura
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
val str = StructType(Array(
	StructField("dia", StringType),
	StructField("distrito", StringType),
	StructField("calle", StringType),
	StructField("victimas", IntegerType),
	StructField("tipo_accidente", StringType),
	StructField("tipo_persona", StringType),
	StructField("sexo", StringType),
	StructField("edad", StringType)
	))
val reg = camposutiles.map(x => Row(x._1, x._2, x._3, x._4, x._5, x._6, x._7, x._8))
val myDF = sqlContext.createDataFrame(reg, str)

import com.databricks.spark.avro._

sqlContext.setConf("spark.sql.avro.compression.codec", "snappy")
myDF.write.avro("/user/cert/simulacros/simulacro8/ej1")

// 2. Crear una tabla en HIVE con los datos del ejercicio 1
hive
use nueva;
create external table accidentes(
dia string,
distrito string,
calle string,
victimas int,
tipo_accidente string,
tipo_persona string,
sexo string,
edad string
)
stored as avro
location '/user/cert/simulacros/simulacro8/ej1'


// 3. Filtrar los accidendes de Vallecas, 
//Contar el total de accidentes que se cometieron por mujeres vs hombres
//Hacer una tabla en mysql con los resultados

val vallecas = myDF.where($"distrito"==="VILLA DE VALLECAS").groupBy("sexo").agg(count("sexo").as("total_accidentes"))
vallecas.rdd.map(_.mkString(",")).saveAsTextFile("/user/cert/simulacros/simulacro8/ej3")

mysql -u cloudera -pcloudera
use ejercicios;

create table accidentes(
sexo varchar(45)
accidentes int
)
;

sqoop export \
--connect jdbc:mysql://localhost:3306/ejercicios \
--username cloudera --password cloudera \
--table accidentes \
--export-dir /user/cert/simulacros/simulacro8/ej3 \
--fields-terminated-by ','

// 4. Realizar un import con la tabla del ejercicio 3
//los campos deben estar separados por tabuladores.
//Guardar en formato texto con gzip. 2 mappers

sqoop import \
--connect jdbc:mysql://localhost:3306/ejercicios \
--username cloudera --password cloudera \
--table accidentes \
--target-dir /user/cert/simulacros/simulacro8/ej4 \
--as-textfile \
--fields-terminated-by '\t' \
--compress --compression-codec org.apache.hadoop.io.compress.GzipCodec \
-m 2

// 5. Sacar el top 5 de calles con mas Atropellos
//Señalar en que distrito se han cometido 
val atropello = myDF.where($"tipo_accidente"==="ATROPELLO")
val res = atropello.groupBy("calle", "distrito").agg(count("calle").as("n_accidentes")).orderBy($"n_accidentes".desc).limit(5)
sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")
res.write.parquet("/user/cert/simulacros/simulacro8/ej5")

// 6. Consultar en Hive. Hallar el distrito con mas nº de victimas que se hayan producido por COLISION DOBLE
// Guardar en mysql con las columnas correspondientes

hive
use nueva;
insert overwrite directory '/user/cert/simulacros/simulacro8/ej6' row format delimited fields terminated by ',' stored as textfile select distrito, sum(victimas) as total_victimas from accidentes where tipo_accidente like "%DOBLE" group by distrito order by total_victimas desc limit 5;

// 7. Consultar que el tipo de Persona sea Conductor, y que el tramo de edad sea entre 21-24 este involucrado en un accidente de atropello
//guardar en avro sin compresion

myDF.registerTempTable("siete")
val resultado = sqlContext.sql("select * from siete where tipo_persona = 'CONDUCTOR' and edad like '%21%24%' and tipo_accidente = 'ATROPELLO'")

sqlContext.setConf("spark.sql.avro.compression.codec", "uncompressed")

resultado.write.avro("/user/cert/simulacros/simulacro8/ej7")