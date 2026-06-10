package com.sensores.main.layers

import com.sensores.main.transformations.{FormatColumnTransformer, HighTemperatureFilter, Transformer}
import com.sensores.main.util.SparkProvider
import com.typesafe.config.ConfigFactory
import io.delta.tables.DeltaTable
import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory


object SilverTransformApp{
  def run(): Unit = {
    val logger = LoggerFactory.getLogger(getClass)
    val config = ConfigFactory.load()
    val kafkaServers = config.getString("kafka.bootstrap.servers")
    val silverCheckpointDir = config.getString("spark.silver.checkpoint.location")
    val bronze_path = config.getString("spark.bronze.path")
    val silver_path = config.getString("spark.silver.path")
    val hadoopHomeDir = config.getString("hadoop.home.dir")

    // Asegúrate de que esta ruta coincida con donde pusiste el bin\winutils.exe
    System.setProperty("hadoop.home.dir", hadoopHomeDir)

    val spark = SparkProvider.getSession(config.getString("spark.app.name"))

    val bronzeDF = spark.readStream.format("delta").load(bronze_path)

    // Aquí defines tu Pipeline de forma declarativa
    val transformations: Seq[Transformer] = Seq(
      new HighTemperatureFilter(30.0),
      new FormatColumnTransformer("TEMPERATURA")
    )

    // Aplicación del pipeline profesional
    val processedDFResult: Either[Throwable, DataFrame] = transformations.foldLeft[Either[Throwable, DataFrame]](Right(bronzeDF)) { (accumulator, transformer) =>
      accumulator.flatMap(df => transformer.transform(df))
    }

    // Ahora extraemos el resultado final
    val finalDF = processedDFResult.getOrElse(throw new RuntimeException("Error en transformaciones"))

    val query = finalDF
      .writeStream
      .trigger(org.apache.spark.sql.streaming.Trigger.ProcessingTime("10 seconds")) // Agrupa datos cada 10s
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>

        // Si el batch está vacío, no hagas nada
        if (!batchDF.isEmpty) {
          // 1. IMPORTANTE: Quedarse solo con el último registro por ID dentro del batch
          import org.apache.spark.sql.expressions.Window
          import org.apache.spark.sql.functions._

          val windowSpec = Window.partitionBy("id").orderBy(col("timestamp").desc)

          val uniqueBatchDF = batchDF
            .withColumn("row_number", row_number().over(windowSpec))
            .filter(col("row_number") === 1)
            .drop("row_number")

          // 2. Ahora sí, hacemos el merge con el dataframe limpio
          uniqueBatchDF.persist()
          try {
            val exists = io.delta.tables.DeltaTable.isDeltaTable(spark, silver_path)

            if (!exists) {
              uniqueBatchDF.write.format("delta").mode("overwrite").save(silver_path)
            } else {
              val targetTable = DeltaTable.forPath(spark, silver_path)
              targetTable.as("oldData")
                .merge(uniqueBatchDF.as("newData"), "oldData.id = newData.id")
                .whenMatched().updateAll()
                .whenNotMatched().insertAll()
                .execute()
            }
          }
          finally {
            uniqueBatchDF.unpersist()
          }
        }
        ()
      }
      .option("checkpointLocation", silverCheckpointDir)
      .start()

    query.awaitTermination()
  }
}