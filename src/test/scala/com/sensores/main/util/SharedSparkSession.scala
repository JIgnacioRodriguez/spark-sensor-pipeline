package com.sensores.main.util

import com.sensores.main.util.SparkProvider
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}

trait SharedSparkSession extends BeforeAndAfterAll { self: Suite =>
  lazy val spark: SparkSession = SparkProvider.getSession("TestApp")
  override def afterAll(): Unit = {
    super.afterAll()
  }
}