package com.example.etl.writer

import com.example.etl.config.EtlJobConfig
import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}
import scala.util.Success
import java.sql.Timestamp
import java.io.File
import org.apache.commons.io.FileUtils

class IcebergWriterSpec extends AnyFlatSpec with Matchers with SharedSparkContext {

  val testWarehousePath = "target/test-warehouse"
  val catalogName = "test_local_catalog"

  override def conf = {
    super.conf.setAppName("IcebergWriterTest")
      .set("spark.sql.session.timeZone", "UTC")
      // Configure a local Iceberg catalog for testing
      .set(s"spark.sql.catalog.${catalogName}", "org.apache.iceberg.spark.SparkCatalog")
      .set(s"spark.sql.catalog.${catalogName}.type", "hadoop") // Using hadoop catalog type for file system
      .set(s"spark.sql.catalog.${catalogName}.warehouse", new File(testWarehousePath).getAbsolutePath)
      .set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    // Clean up warehouse path before tests
    FileUtils.deleteDirectory(new File(testWarehousePath))
  }

  override def afterAll(): Unit = {
    // Clean up warehouse path after tests
    FileUtils.deleteDirectory(new File(testWarehousePath))
    super.afterAll()
  }


  def createDummyConfig(namespace: String, tableName: String, partitionCols: List[String] = List.empty): EtlJobConfig = {
    EtlJobConfig(
      jobId = "testIcebergJob",
      jobDescription = Some("Test Iceberg Job"),
      sourceFilePath = "dummy/path", // Not used directly by this writer test
      sourceFileType = "CSV",
      sourceFileDelimiter = Some(","),
      sourceHasHeader = true,
      targetIcebergNamespace = namespace,
      targetIcebergTableName = tableName,
      targetIcebergPartitionColumns = partitionCols,
      businessKeyColumns = List.empty,
      scd2StartDateColumnName = "eff_start",
      scd2EndDateColumnName = "eff_end",
      scd2CurrentFlagColumnName = "is_current",
      asIsLoad = false,
      schemaMappingJson = None,
      businessTransformationRulesJson = None,
      dataQualityRulesJson = None,
      isActive = true,
      createdTs = new Timestamp(System.currentTimeMillis()),
      updatedTs = new Timestamp(System.currentTimeMillis())
    )
  }

  "IcebergWriter" should "write a DataFrame to a new Iceberg table" in {
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    val data = Seq(Row(1, "Alice", "HR"), Row(2, "Bob", "Engineering"))
    val schema = StructType(List(
      StructField("id", IntegerType, nullable = false),
      StructField("name", StringType, nullable = true),
      StructField("department", StringType, nullable = true)
    ))
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

    val namespace = "test_ns"
    val tableName = "test_table_1"
    val config = createDummyConfig(namespace, tableName)

    // Ensure the DataFrame is not empty before writing
    df.count() should be > 0


    val result = IcebergWriter.writeToIceberg(spark, df, config, catalogName)
    result shouldBe a[Success[_]]

    // Verify data by reading it back
    val loadedDf = spark.read.format("iceberg").load(s"${catalogName}.${namespace}.${tableName}")
    loadedDf.count() should be (2)
    loadedDf.select("name").as[String].collect() should contain allOf ("Alice", "Bob")

    spark.sql(s"DROP TABLE IF EXISTS ${catalogName}.${namespace}.${tableName}")
    spark.stop()
  }

  "IcebergWriter" should "write a DataFrame to an Iceberg table with partitioning" in {
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    val data = Seq(
      Row(1, "Alice", "HR", "2023-01"),
      Row(2, "Bob", "Engineering", "2023-01"),
      Row(3, "Charlie", "HR", "2023-02")
    )
    val schema = StructType(List(
      StructField("id", IntegerType, nullable = false),
      StructField("name", StringType, nullable = true),
      StructField("department", StringType, nullable = true),
      StructField("month", StringType, nullable = true) // Partition column
    ))
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

    df.count() should be > 0


    val namespace = "test_ns"
    val tableName = "test_table_partitioned"
    val config = createDummyConfig(namespace, tableName, partitionCols = List("department", "month"))

    val result = IcebergWriter.writeToIceberg(spark, df, config, catalogName)
    result shouldBe a[Success[_]]

    val loadedDf = spark.read.format("iceberg").load(s"${catalogName}.${namespace}.${tableName}")
    loadedDf.count() should be (3)
    loadedDf.select("name").as[String].collect() should contain allOf ("Alice", "Bob", "Charlie")

    // Check partition discovery (more involved, this is a basic check)
    val partitions = spark.sql(s"SELECT * FROM ${catalogName}.${namespace}.${tableName}.partitions")
    partitions.count() should be (3) // HR/2023-01, Eng/2023-01, HR/2023-02 - wait, partitionBy combines them if distinct values are same.
                                        // (HR, 2023-01), (Engineering, 2023-01), (HR, 2023-02) -> 3 distinct partitions

    // Let's verify the created table's partition spec
    val table = org.apache.iceberg.catalog.TableIdentifier.of(namespace, tableName)
    // This requires access to Iceberg Catalog API, which might be tricky here.
    // For now, count of partitions is a good indicator.
    // The actual partitions are (HR, 2023-01), (Engineering, 2023-01), (HR, 2023-02)
    // This means partitions.count() should be 3.

    // Reread and check count based on filter
    val hrCount = spark.read.format("iceberg").load(s"${catalogName}.${namespace}.${tableName}").where("department = 'HR'").count()
    hrCount should be (2)

    spark.sql(s"DROP TABLE IF EXISTS ${catalogName}.${namespace}.${tableName}")
    spark.stop()
  }

  "IcebergWriter" should "sanitize column names before writing" in {
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    // Create DataFrame with problematic column names
    val data = Seq(Row(1, "Alice"), Row(2, "Bob"))
    val schema = StructType(List(
      StructField("id number (pk)", IntegerType, nullable = false),
      StructField("user name", StringType, nullable = true)
    ))
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

    df.count() should be > 0

    val namespace = "test_ns"
    val tableName = "test_table_sanitize"
    val config = createDummyConfig(namespace, tableName)

    val result = IcebergWriter.writeToIceberg(spark, df, config, catalogName)
    result shouldBe a[Success[_]]

    val loadedDf = spark.read.format("iceberg").load(s"${catalogName}.${namespace}.${tableName}")
    loadedDf.count() should be (2)
    loadedDf.columns should contain allOf ("id_number_pk_", "user_name") // Check sanitized names

    spark.sql(s"DROP TABLE IF EXISTS ${catalogName}.${namespace}.${tableName}")
    spark.stop()
  }
}
