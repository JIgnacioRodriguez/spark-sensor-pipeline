package com.sensores.main.layers

import com.sensores.main.util.SparkProvider
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions._
import io.delta.tables.DeltaTable

object GoldAggregationApp {
  def run(): Unit = {
    val config = ConfigFactory.load()
    val silver_path = config.getString("spark.silver.path")
    val gold_path = config.getString("spark.gold.path")
    val goldCheckpointDir = config.getString("spark.gold.checkpoint.location")

    val spark = SparkProvider.getSession(config.getString("spark.app.name"))

    val silverDF = spark.readStream
      .format("delta")
      .option("skipChangeCommits", "true")
      .load(silver_path)

    val goldDF = silverDF.withColumn("timestamp", col("timestamp").cast("timestamp"))
      .withWatermark("timestamp", "1 minute")
      .groupBy(window(col("timestamp"), "1 minute"), col("sensor"))
      .agg(
        avg("temperatura").as("temp_media"),
        max("temperatura").as("temp_max"),
        count("*").as("total_lecturas")
      )

    val query = goldDF
      .writeStream
      .format("delta")
      .outputMode("update")
      .option("checkpointLocation", goldCheckpointDir)
      .foreachBatch { (batchDF: org.apache.spark.sql.DataFrame, batchId: Long) =>

        if (!DeltaTable.isDeltaTable(spark, gold_path)) {
          batchDF.write.format("delta").save(gold_path)
        } else {
          val targetTable = DeltaTable.forPath(spark, gold_path)

          targetTable.as("target")
            .merge(batchDF.as("source"),
              "target.window = source.window AND target.sensor = source.sensor")
            .whenMatched()
            .updateExpr(Map(
              "temp_media" -> "source.temp_media",
              "temp_max" -> "source.temp_max",
              "total_lecturas" -> "source.total_lecturas"
            ))
            .whenNotMatched()
            .insertAll()
            .execute()
        }
      }
      .start()

    query.awaitTermination()
  }
}