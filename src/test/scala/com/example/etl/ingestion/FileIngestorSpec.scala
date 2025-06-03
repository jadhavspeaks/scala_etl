package com.example.etl.ingestion

import com.example.etl.config.EtlJobConfig
import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.SparkSession
import scala.util.Success
import java.sql.Timestamp

class FileIngestorSpec extends AnyFlatSpec with Matchers with SharedSparkContext {

  override def conf = {
    super.conf.setAppName("FileIngestorTest")
      .set("spark.sql.session.timeZone", "UTC")
  }

  // Helper to create a dummy EtlJobConfig for testing
  def createDummyConfig(filePath: String, fileType: String, delimiter: Option[String] = None, hasHeader: Boolean = true): EtlJobConfig = {
    EtlJobConfig(
      jobId = "testJob",
      jobDescription = Some("Test Job"),
      sourceFilePath = filePath,
      sourceFileType = fileType,
      sourceFileDelimiter = delimiter,
      sourceHasHeader = hasHeader,
      targetIcebergNamespace = "test_ns",
      targetIcebergTableName = "test_table",
      targetIcebergPartitionColumns = List.empty,
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

  "FileIngestor" should "ingest a CSV file with header" in {
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val testCsvPath = getClass.getResource("/sample_header.csv").getPath
    val config = createDummyConfig(testCsvPath, "CSV", Some(","), true)

    val result = FileIngestor.ingestFile(spark, config)

    result shouldBe a[Success[_]]
    val df = result.get
    df.columns should contain allOf ("id", "name", "value")
    df.count() should be (2)
    df.schema.fields.map(_.name) should equal (Array("id", "name", "value")) // inferSchema makes them string by default here
    spark.stop() // Stop spark session to release resources
  }

  "FileIngestor" should "ingest a TXT file (as CSV) without header and with pipe delimiter" in {
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val testTxtPath = getClass.getResource("/sample_noheader.txt").getPath
    val config = createDummyConfig(testTxtPath, "TXT", Some("|"), false)

    val result = FileIngestor.ingestFile(spark, config)

    result shouldBe a[Success[_]]
    val df = result.get
    df.columns should contain allOf ("_c0", "_c1", "_c2") // Default column names
    df.count() should be (3)
    spark.stop()
  }

  // Add more tests: e.g., file not found, unsupported type
}
