package com.sensores.main

import com.sensores.main.layers.{BronzeIngestApp, SilverTransformApp, GoldAggregationApp}
import org.slf4j.LoggerFactory

object Main {
  def main(args: Array[String]): Unit = {
    val logger = LoggerFactory.getLogger(getClass)

    if (args.length == 0) {
      logger.error("Error: Debes especificar una capa: bronze, silver o gold")
      sys.exit(1)
    }

    args(0).toLowerCase match {
      case "bronze" => BronzeIngestApp.run()
      case "silver" => SilverTransformApp.run()
      case "gold"   => GoldAggregationApp.run()
      case _        => logger.error("Capa desconocida")
    }
  }
}