// 1. Importar la tabla orders 
//solo los registros con order_status CLOSED y 
//con order_customer_id 11000. 
//En formato parquet + snappy. Con un mapper

sqoop import \
--connect jdbc:mysql://localhost:3306/retail_db \
--username cloudera --password cloudera \
--table orders \
--target-dir /user/cert/simulacro7/ej1 \
--where "order_customer_id > 11000 and order_status = 'CLOSED'" \
--as-parquetfile \
--compress --compression-codec org.apache.hadoop.io.compress.SnappyCodec \
-m 1

// 2. Leer la tabla fbi.resultado_csv de HIVE. 
//Realizar un filtro para quedarte con los registros a partir del 2003.
//Guardar en el fichero de texto separado por "#". Sin compresión

val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)

val lectura = hiveContext.table("fbi.resultado_csv")

val filtro = lectura.where($"anyo" >= 2003)

val resultado = filtro.rdd.map(x => x(0) + "#" + x(1))

resultado.coalesce(1).saveAsTextFile("/user/cert/simulacro7/ej2")

// 3. Con los ficheros de movielens
//Encontrar la media de rating de las peliculas, ademas del numero de votos a cada una. Ordenar por numero de votos desc, rating desc. Guardar en avro + snappy

val rat = sc.textFile("/user/cloudear/data/movielens/ratings.csv")
val mov = sc.textFile("/user/cloudear/data/movielens/movies.csv")
//ambos ficheros tienen cabecera, tenemos que quitarsela

val cab1 = rat.first  // userId, movieId, rating, timestamp
val ratings = rat.filter(x => x != cab1).map(_.split(','))

val cab2 = mov.first  // movieId, title, genres
val movies = mov.filter(x => x != cab2).map(_.split(','))

//vamos a usar por ahora ratings, para hacer todos los calculos
//luego la enlazaremos con movies para saber el nombre de la peliculas

val oper = ratings.map(x => (x(1), (1, x(2).toFloat))).
			reduceByKey((x,y) => (x._1+y._1, x._2+y._2)).
			map(x => (x._1, x._2._1, x._2._2.toFloat/x._2._1))
// ya tenemos una tupla con (idpelicula, votos, mediarating)
//vamos a darle una estructura, para darle un orden en votos y para poder guardar en avro

import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

val str1 = StructType(Array(
	StructField("movieId", StringType),
	StructField("votos", IntegerType),
	StructField("media", FloatType)
	))
val reg1 = oper.map(x => Row(x._1, x._2, x._3))
val ratDF = sqlContext.createDataFrame(reg1, str1)

val str2 = StructType(Array(
	StructField("movieId", StringType),
	StructField("titulo", StringType)
	))
val reg2 = movies.map(x => Row(x(0),x(1)))
val movDF = sqlContext.createDataFrame(reg2, str2)
//vamos a unirlas
val joinDF = ratDF.join(movDF, ratDF("movieId")===movDF("movieId"))
val solution = joinDF.groupBy("titulo").
		agg(sum("votos").as("total_votos"), sum("media").as("puntuacion_media")).
		orderBy($"total_votos".desc)
//a guardarlo en avro+snappy
import com.databricks.spark.avro._
sqlContext.setConf("spark.sql.avro.compression.codec", "snappy")
solution.coalesce(1).write.avro("/user/cert/simulacro7/ej3")
// 4. Hacer una exportacion de 3) a Hive. Usando el esquema, no valen trampas
hdfs dfs -get /user/cert/simulacro7/ej3/part-r-00000-11d30179-711d-4aa2-8a7b-8ea96347cc91.avro /home/cloudera/Desktop

avro-tools getschema /home/cloudera/Desktop/part-r-00000-11d30179-711d-4aa2-8a7b-8ea96347cc91.avro > /home/cloudera/Desktop/esq1.avsc

hdfs dfs -put /home/cloudera/Desktop/esq1.avsc /user/cert/simulacro7/esquema

hive
use problem3;
create external table peliculas
    > stored as avro
    > location '/user/cert/simulacro7/ej3'
    > tblproperties ("avro.schema.url" = "/user/cert/simulacro7/esquema/esq1.avsc")
    > ;


// 5. Exportacion de 3) a mysql

//lo tenemos en avro, para poder exportar a mysql tiene que ser csv
import com.databricks.spark.avro._
val ej3 = sqlContext.read.avro("user/cert/simulacro7/ej3")
val csv = ej3.rdd.map(x => x(0) + "," + x(1) + "," + x(2).toString.toFloat)
csv.saveAsTextFile("/user/cert/simulacro7/ej5")
//ahora hacemos la tabla en mysql para poder exportar los datos

mysql -u cloudera -pcloudera
use ejercicios;
create table odio(
titulo varchar(255),
votos int,
media float)

//exportacion
sqoop export \
--connect jdbc:mysql://localhost:3306/ejercicios \
--username cloudera --password cloudera \
--table odio \
--export-dir /user/cert/simulacro7/ej5 \
--fields-terminated-by ','

// 6. De la tabla orders, importada en 1),
//sacar los que tengan un order_id acabado en 3.  Guardar en Json + gzip

val ord = sqlContext.read.parquet("/user/cert/simulacro7/ej1")
ord.registerTempTable("ordenes")

val filtro = sqlContext.sql("select * from ordenes where order_id like '%3'")

filtro.coalesce(1).toJSON.saveAsTextFile("/user/cert/simulacro7/ej6", classOf[org.apache.hadoop.io.compress.GzipCodec])



// 7. top 10 de peliculas de terror con mas rating, minimo 20 votos. guardar en *sv

val rat = sc.textFile("/user/cloudear/data/movielens/ratings.csv")
val mov = sc.textFile("/user/cloudear/data/movielens/movies.csv")

//ambos ficheros tienen cabecera, tenemos que quitarsela, de paso le filtramos el genero terror

val cab1 = rat.first  // userId, movieId, rating, timestamp
val ratings = rat.filter(x => x != cab1).map(_.split(','))

val cab2 = mov.first  // movieId, title, genres
val movies = mov.filter(x => x != cab2).map(_.split(',')).filter(x => x(2).contains("Horror"))

//les damos estructura

import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

val strat = StructType(Array(
	StructField("userId", StringType),
	StructField("movieId", StringType),
	StructField("rating", FloatType)
	))
val regrat = ratings.map(x => Row(x(0), x(1), x(2).toString.toFloat))
val ratDF = sqlContext.createDataFrame(regrat, strat)

val stmov = StructType(Array(
	StructField("movieId", StringType),
	StructField("title", StringType),
	StructField("genres", StringType)
	))
val regmov = movies.map(x => Row(x(0), x(1), x(2)))
val movDF = sqlContext.createDataFrame(regmov, stmov)

//las juntamos 
val joinDF = ratDF.join(movDF, ratDF("movieId")===movDF("movieId"))

val calculos = joinDF.groupBy("title").agg(countDistinct("userId").as("votos"), avg("rating").as("calificacion_media")).
				where($"votos" > 20).orderBy($"calificacion_media".desc, $"votos".desc).limit(10)

val texto = calculos.rdd.map(x => x(0) + "*" + x(1) + "*" + x(2))

texto.saveAsTextFile("/user/cert/simulacro7/ej7")