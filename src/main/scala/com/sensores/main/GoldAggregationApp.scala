package com.sensores.main

import com.sensores.main.util.SparkProvider
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

object GoldAggregationApp {
  def main(args: Array[String]): Unit = {
    val config = ConfigFactory.load()
    val silver_path = config.getString("spark.silver.path")
    val gold_path = config.getString("spark.gold.path")
    val goldCheckpointDir = config.getString("spark.gold.checkpoint.location")

    val spark = SparkProvider.getSession(config.getString("spark.app.name"))

    // Leemos de Silver
    val silverDF = spark.readStream.format("delta").load(silver_path)

    // CONVERSIÓN: Aquí obligamos a Spark a tratar la columna como fecha real
    val preparedDF = silverDF.withColumn("timestamp", col("timestamp").cast("timestamp"))

    // Agregación Gold
    val goldDF = preparedDF
      .withWatermark("timestamp", "1 minute")
      .groupBy(
        window(col("timestamp"), "1 minute"),
        col("sensor")
      )
      .agg(
        avg("temperatura").as("temp_media"),
        max("temperatura").as("temp_max"),
        count("*").as("total_lecturas")
      )

    // Escritura a Gold
    val query = goldDF
      .writeStream
      .format("delta")
      .outputMode("append")
      .option("checkpointLocation", goldCheckpointDir)
      .start(gold_path)

    // ¡CRÍTICO!: Esto mantiene el programa vivo procesando el stream
    query.awaitTermination()
  }
}