package com.sensores.main

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import com.typesafe.config.ConfigFactory
import org.slf4j.LoggerFactory
import com.sensores.main.util.SparkProvider


object BronzeIngestApp{
  def main(args: Array[String]): Unit = {
    val logger = LoggerFactory.getLogger(getClass)
    val config = ConfigFactory.load()
    val kafkaServers = config.getString("kafka.bootstrap.servers")
    val bronzeCheckpointDir = config.getString("spark.bronze.checkpoint.location")
    val bronzePath = config.getString("spark.bronze.path")
    val hadoopHomeDir = config.getString("hadoop.home.dir")

    import org.apache.kafka.clients.admin.AdminClient
    import java.util.Properties

    val props = new Properties()
    props.put("bootstrap.servers", kafkaServers)
    val adminClient = AdminClient.create(props)
    val descriptions = adminClient.describeTopics(java.util.Collections.singleton("sensores-data")).all().get()
    val partitions = descriptions.get("sensores-data").partitions().size()

    logger.debug(s"DEBUG: El tópico de Kafka tiene $partitions particiones reales.")

    // Asegúrate de que esta ruta coincida con donde pusiste el bin\winutils.exe
    System.setProperty("hadoop.home.dir", hadoopHomeDir)

    // Define el esquema de tu JSON para poder consultar campos individuales
    val schema = new StructType()
      .add("id", IntegerType)
      .add("timestamp", StringType)
      .add("sensor", StringType)
      .add("temperatura", DoubleType)
      .add("humedad", DoubleType)

    // En tu código de Spark, esto es lo que le indica al driver cómo comportarse:
    val spark = SparkProvider.getSession(config.getString("spark.app.name"))

    // Convierte la columna 'value' (que es binaria/string) a columnas reales
    val rawDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaServers)
      .option("subscribe", "sensores-data")
      .load()
      .select(from_json(col("value").cast("string"), schema).as("data"))
      .select("data.*") // Esto expande el JSON en columnas separadas

    val query = rawDF
      .writeStream
      .format("delta")
      .option("path", bronzePath) // Carpeta donde se guardarán los archivos
      .option("checkpointLocation", bronzeCheckpointDir)
      .outputMode("append") // Añadir nuevos registros al conjunto existente
      .start()

    query.awaitTermination()
  }
}