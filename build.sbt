name := "metadata-driven-etl-framework"
version := "0.1.0-SNAPSHOT"

scalaVersion := "2.12.15" // Or a newer 2.12.x compatible with your Spark version

val sparkVersion = "3.3.0" // Or your target Spark version
val icebergVersion = "1.5.0" // Or your target Iceberg version
val oracleJdbcVersion = "21.5.0.0" // Or a compatible version
val sparkExcelVersion = "0.14.0" // Check for latest compatible version with Spark 3.3.0

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.iceberg" %% "iceberg-spark-runtime-3.3" % icebergVersion, // Ensure Spark version matches
  "com.oracle.database.jdbc" % "ojdbc8" % oracleJdbcVersion, // Or ojdbc11 depending on your JDK
  "com.crealytics" %% "spark-excel" % sparkExcelVersion,
  // For data quality and parsing, consider:
  // "com.univocity" % "univocity-parsers" % "2.9.1",

  // Testing
  "org.scalatest" %% "scalatest" % "3.2.14" % Test,
  "com.holdenkarau" %% "spark-testing-base" % s"${sparkVersion}_1.1.1" % Test, // Check for compatible version

  // Logging
  "org.slf4j" % "slf4j-api" % "1.7.32",
  "ch.qos.logback" % "logback-classic" % "1.2.10",

  // Apache Commons IO for testing (e.g. FileUtils)
  "commons-io" % "commons-io" % "2.11.0" % Test,

  // Spray JSON for parsing config JSON
  "io.spray" %% "spray-json" % "1.3.6"
)

// Resolver for Oracle JDBC if not in default Maven central (though ojdbc8 often is)
// resolvers += "Oracle Maven Repository" at "https://maven.oracle.com/repository/maven/release/"
