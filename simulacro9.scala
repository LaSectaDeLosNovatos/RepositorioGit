// simulacro9

// 1. Cargar en HDFS el fichero colegios.csv. Con spark, quedarnos las columnas PK, NOMBRE, EQUIPAMIENTO, ACCESIBILIDAD, NOMBRE-VIA, CLASE-VIAL, NUM, CODIGO-POSTAL, BARRIO, DISTRITO. guardar en csv

hdfs dfs -mkdir /user/cert/simulacros/simulacro9
hdfs dfs -mkdir /user/cert/simulacros/simulacro9/data
hdfs dfs -put /home/cloudera/Desktop/colegios.csv /user/cert/simulacros/simulacro9/data

spark-shell

val lectura = sc.textFile("/user/cert/simulacros/simulacro9/data")

val cabecera = lectura.first

val body = lectura.filter(x => x != cabecera).map(_.split(";"))
val camposutiles = body.map(x => (x(0), x(1), x(2), x(4),x(5),x(6),x(8),x(11),x(12),x(13)))
val csv = camposutiles.map(x => x._1 + "," + x._2 + "," + x._3 + "," + x._4 + "," + x._5 + "," + x._6 + "," + x._7 + "," + x._8 + "," + x._9 + "," + x._10)

csv.saveAsTextFile("/user/cert/simulacros/simulacro9/ej1")
// 2. Exportar el resultado a mysql
mysql -u cloudera -pcloudera

use ejercicios;

create table colegios(
codigo int,
colegio varchar(255),
equipamiento varchar(255),
accesibilidad varchar(45),
nombre_via varchar(255),
clase_via varchar(45),
numero int,
codigo_postal int,
barrio varchar(45),
distrito varchar(45))


sqoop export \
--connect jdbc:mysql://localhost:3306/ejercicios \
--username cloudera --password cloudera \
--table colegios \
--export-dir /user/cert/simulacros/simulacro9/ej1 \
--fields-terminated-by ','


// 3. Importar los datos de mysql cuya CLASE-VIAL sea calle. formato avro+snappy. 1 map
sqoop import \
--connect jdbc:mysql://localhost:3306/ejercicios \
--username cloudera --password cloudera \
--table colegios \
--target-dir /user/cert/simulacros/simulacro9/ej3 \
--where "clase_via = 'CALLE'" \
--as-avrodatafile \
--compress \
--compression-codec org.apache.hadoop.io.compress.SnappyCodec \
-m 1

// 4. Con lo importado en 3, encontrar el numero de colegios por distrito. Guardar en parquet con gzip

import com.databricks.spark.avro._

val colDF = sqlContext.read.avro("/user/cert/simulacros/simulacro9/ej3")

val results = colDF.groupBy("distrito").agg(count("colegio").as("colegios_por_distrito"))

sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")
results.write.parquet("/user/cert/simulacros/simulacro9/ej4")
// 5. con lo importado en 3, encontrar cuantos barrios distintos por distrito tienen colegios, y cuantos colegios tienen esos barrios. Guarcar en parquet sin compresion
val colDF = sqlContext.read.avro("/user/cert/simulacros/simulacro9/ej3")

val barDF = colDF.groupBy("distrito").agg(countDistinct("barrio").as("barrios_con_colegio"), count("barrio").as("colegios_por_distrito"))

sqlContext.setConf("spark.sql.parquet.compression.codec", "uncompressed")

barDF.write.parquet("/user/cert/simulacros/simulacro9/ej5")
// 6. Con lo importado en 3, encontrar todos los colegios con accesibilidad de tipo 1. Guardar en JSON + gzip

val colDF = sqlContext.read.avro("/user/cert/simulacros/simulacro9/ej3")
val accesibles = colDF.where($"accesibilidad"==="1")

accesibles.coalesce(1).toJSON.saveAsTextFile("/user/cert/simulacros/simulacro9/ej6")

// 7. con los datos del 3, encontrar los colegios que cuentan con Comedor entre su equipamiento. Guardar en TSV 

colDF.registerTempTable("coles")
val comederos = sqlContext.sql("select * from coles where equipamiento like '%Comedor%'")

val solution = comederos.map(x => x.mkString("\t"))

solution.saveAsTextFile("/user/cert/simulacros/simulacro9/ej7")