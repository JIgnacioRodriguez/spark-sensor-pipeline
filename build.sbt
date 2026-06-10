name := "ProcesamientoBigData"
version := "1.1.3"
scalaVersion := "2.12.18"

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

// Lógica inteligente: Si la variable de entorno DEPLOY_MODE es "prod", aplica "provided"
val sparkProvided = sys.env.get("DEPLOY_MODE").contains("prod")

libraryDependencies ++= Seq(
  if (sparkProvided) "org.apache.spark" %% "spark-sql" % "3.5.1" % "provided"
  else "org.apache.spark" %% "spark-sql" % "3.5.1",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.1",
  "org.apache.kafka" % "kafka-clients" % "3.5.0",
  "com.typesafe" % "config" % "1.4.2",
  "io.delta" %% "delta-spark" % "3.1.0"
)
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.17" % Test

Test / javaOptions ++= Seq(
  "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/sun.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "-Xmx2G"
)

assembly / mainClass := Some("com.sensores.main.Main")

Compile / run / fork := true
Test / fork := true