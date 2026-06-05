package com.sensores.main.util

import org.apache.spark.sql.SparkSession

object SparkProvider {
  def getSession(appName: String): SparkSession = {
    SparkSession.getActiveSession.getOrElse {
      SparkSession.builder()
        .appName(appName)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.extraJavaOptions", "-XX:+IgnoreUnrecognizedVMOptions --add-modules=jdk.unsupported")
        .getOrCreate()
    }
  }
}