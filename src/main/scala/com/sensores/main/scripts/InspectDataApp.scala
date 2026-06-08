package com.sensores.main.scripts

import com.typesafe.config.ConfigFactory
import com.sensores.main.util.SparkProvider
import org.apache.spark.sql.functions.col


object InspectDataApp {
  def main(args: Array[String]): Unit = {
    val config = ConfigFactory.load()
    val hadoopHomeDir = config.getString("hadoop.home.dir")

    System.setProperty("hadoop.home.dir", hadoopHomeDir)
    val spark = SparkProvider.getSession("ParquetReader")

    val dfLectura = spark.read.format("delta").load(config.getString("spark.silver.path"))
    dfLectura.orderBy(col("id")).show()
  }
}