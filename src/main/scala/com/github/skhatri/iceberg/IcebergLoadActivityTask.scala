package com.github.skhatri.iceberg

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DateType, DecimalType, DoubleType, StringType, StructField, StructType, TimestampType}

object IcebergLoadActivityTask extends App {
  private val sparkBuilder = SparkSession.builder()
    .appName("iceberg-spark-session")
    .master("local[2]")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.iceberg.warehouse", "./warehouse")
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.spark_catalog.warehouse", "./warehouse")

  private val sparkCatalog = System.getenv("CATALOG_URL") match {
    case url: String if url.startsWith("http") => sparkBuilder
      .config("spark.sql.catalog.iceberg.type", "rest")
      .config("spark.sql.catalog.iceberg.uri", "http://localhost:8181")
      .config("spark.sql.catalog.spark_catalog.uri", "http://localhost:8181")
      .config("spark.sql.catalog.spark_catalog.type", "rest")
    case _ => sparkBuilder
      .config("spark.sql.catalog.iceberg.type", "jdbc")
      .config("spark.sql.catalog.iceberg.uri", "jdbc:sqlite:file:/tmp/iceberg_rest_mode=memory")
      .config("spark.sql.catalog.spark_catalog.type", "jdbc")
      .config("spark.sql.catalog.spark_catalog.uri", "jdbc:sqlite:file:/tmp/spark_rest_mode=memory")
  }

  private val spark = sparkCatalog.getOrCreate()

  private val tableName = "iceberg.finance.activity"
  spark.sql(s"drop table if exists $tableName")
  //account,txn_date,txn_id,merchant,amount,category,last_updated
  spark.sql(
    s"""create table if not exists $tableName(
       |account string,
       |txn_date date,
       |txn_id string,
       |merchant string,
       |amount double,
       |category string,
       |last_updated timestamp
       |) USING iceberg
       |TBLPROPERTIES(
       |   'write.wap.enabled'='true',
       |   'write.delete.mode'='copy-on-write',
       |   'write.update.mode'='merge-on-read',
       |   'write.merge.mode'='merge-on-read',
       |   'read.parquet.vectorization.enabled'='true',
       |   'write.format.default'='parquet',
       |   'write.delete.format.default'='avro',
       |   'write.parquet.compression-codec'='zstd',
       |   'write.metadata.delete-after-commit.enabled'='true',
       |   'write.metadata.previous-versions-max'='80',
       |   'comment'='Transaction Table'
       |)
       |partitioned by (txn_date) """.stripMargin)


  private[this] def loadDataFile(name: String, branch: String = ""): Unit = {
  val tmpTableName = "iceberg.finance.activity_tmp"
    spark.sql(
      s"""create or replace table $tmpTableName USING iceberg
        |PARTITIONED BY (txn_date)
        |AS select * from $tableName limit 0
        |""".stripMargin)

    val activities = spark.read.format("csv")
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
      .load(s"src/main/resources/input/$name")
      .writeTo(tmpTableName)
      .option("mergeSchema", "true")

    activities.append()
    spark.sql(
      s"""MERGE INTO $tableName as target
        |USING $tmpTableName as src
        |ON target.txn_id = src.txn_id
        |WHEN MATCHED THEN
        | UPDATE SET target.amount = src.amount, target.category = src.category, target.last_updated = src.last_updated, target.txn_date = src.txn_date
        |WHEN NOT MATCHED THEN
        | INSERT *
        |""".stripMargin)

    if (branch.nonEmpty) {
      spark.sql(s"ALTER TABLE $tableName CREATE OR REPLACE BRANCH `$branch`")
    }
    spark.sql(s"DROP TABLE $tmpTableName")
  }

  private[this] def printTableMetadata(): Unit = {

    println("Activity History")
    spark.sql(s"SELECT * from $tableName.history").show(10, truncate = false)

    println("Metadata Log Entries")
    spark.sql(s"SELECT * from $tableName.metadata_log_entries").show(10, truncate = false)

    println("Snapshots")
    spark.sql(s"SELECT * from $tableName.snapshots").show(10, truncate = false)

    println("Manifests")
    spark.sql(s"SELECT * from $tableName.entries").show(10, truncate = false)

    println("Files")
    spark.sql(s"SELECT * from $tableName.files").show(10, truncate = false)

  }

  loadDataFile("2024-03-02", "day1")

  loadDataFile("2024-03-03", "day2")

  loadDataFile("2024-03-05", "day3")
  loadDataFile("2024-03-08", "day4")
  loadDataFile("2024-03-09", "day5")

  private val activityTable = spark.table(tableName)
  println(s"total rows: ${activityTable.count()}")

  activityTable.foreach(println)
  spark.sql(s"SELECT * from $tableName.history").show(10, truncate = false)

  private[this] def countTableData(watermark: String = "latest"): Unit = {
    val data = spark.sql(if (watermark == "" || watermark == "latest") s"select * from $tableName" else s"select * from $tableName VERSION AS OF '$watermark'")
    println(s"watermark: ${watermark}, count=${data.count()}")
  }

  countTableData("day1")
  countTableData("day2")
  countTableData("day3")
  countTableData("day4")
  countTableData("day5")
  countTableData("latest")

  spark.sql(s"select account, count(*) as activity_count from ${tableName} group by account").show(10, truncate = false)

  println("""As of version 2024-03-02, what was transaction id txn10 labelled as?""".stripMargin)
  spark.sql(
    s"""select * from $tableName VERSION AS OF 'day1'
       |where txn_id='txn10'
       |""".stripMargin).show(2, truncate = false)

  println("""As of version 2024-03-09, what was transaction id txn10 labelled as?""".stripMargin)
  spark.sql(
    s"""select * from $tableName VERSION AS OF 'day5'
       |where txn_id='txn10'
       |""".stripMargin).show(2, truncate = false)

  println("""What is the latest category of transaction id txn10 and when was it last updated?""".stripMargin)
  spark.sql(
    s"""select * from $tableName
       |where txn_id='txn10'
       |""".stripMargin).show(2, truncate = false)

  println("""Acc5 bought something from Apple Store Sydney on 2024-03-05, how did the category for this transaction change over time?""".stripMargin)
  spark.sql(
    s"""select *
       |from $tableName VERSION as of 'day3'
       |where account = 'acc5' and txn_date=cast('2024-03-05' as date) and merchant='Apple Store Sydney'""".stripMargin).show(2, truncate = false)

  spark.sql(
    s"""select *
       |from $tableName
       |where account = 'acc5' and txn_date=cast('2024-03-05' as date) and merchant='Apple Store Sydney'""".stripMargin).show(2, truncate = false)


  spark.close()
}
