package com.github.skhatri.iceberg

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DateType, DecimalType, DoubleType, StringType, StructField, StructType, TimestampType}

object IcebergLoadActivityTask extends App {
  private val sparkBuilder = SparkSession.builder()
    .appName("iceberg-spark-session")
    .master("local[2]")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkSessionCatalog")
    .config("spark.sql.catalog.iceberg.warehouse", "./warehouse")

  private val sparkCatalog = System.getenv("CATALOG_URL") match {
    case url: String if url.startsWith("http") => sparkBuilder.config("spark.sql.catalog.iceberg.type", "rest")
      .config("spark.sql.catalog.iceberg.uri", "http://localhost:8181")
    case _ => sparkBuilder.config("spark.sql.catalog.iceberg.type", "jdbc")
      .config("spark.sql.catalog.iceberg.uri", "jdbc:sqlite:file:/tmp/iceberg_rest_mode=memory")
  }

  private val spark = sparkCatalog.getOrCreate()

  //account,txn_date,txn_id,merchant,amount,category,last_updated
  spark.sql(
    """create table if not exists iceberg.finance.activity(
      |account string,
      |txn_date date,
      |txn_id string,
      |merchant string,
      |amount double,
      |category string,
      |last_updated timestamp
      |) using iceberg
      |partitioned by (txn_date)""".stripMargin)


  private val activities = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .schema(StructType(Seq[StructField](
      StructField("account", StringType),
      StructField("txn_date", DateType),
      StructField("txn_id", StringType),
      StructField("merchant", StringType),
      StructField("amount", DoubleType),
      StructField("category", StringType),
      StructField("last_updated", TimestampType)
    )))
    .load("src/main/resources/input/2024-03-02")

  activities.writeTo("iceberg.finance.activity").partitionedBy(col("txn_date")).createOrReplace()
  private val activityTable = spark.table("iceberg.finance.activity")
  println(s"total rows: ${activityTable.count()}")


  activityTable.foreach(println)

  println("Activity History")
  spark.sql("SELECT * from iceberg.finance.activity.history").show(10, false)

  println("Metadata Log Entries")
  spark.sql("SELECT * from iceberg.finance.activity.metadata_log_entries").show(10, false)

  println("Snapshots")
  spark.sql("SELECT * from iceberg.finance.activity.snapshots").show(10, false)

  println("Manifests")
  spark.sql("SELECT * from iceberg.finance.activity.entries").show(10, false)

  println("Files")
  spark.sql("SELECT * from iceberg.finance.activity.files").show(10, false)
  spark.close()
}
