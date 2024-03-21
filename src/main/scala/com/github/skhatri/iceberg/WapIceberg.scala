package com.github.skhatri.iceberg

import com.github.skhatri.iceberg.IcebergLoadActivityTask.spark
import com.github.skhatri.iceberg.WapIceberg.eodDate
import org.apache.spark.sql.types.{DateType, DoubleType, StringType, StructField, StructType, TimestampType}

import java.util.{Random, UUID}


object WapIceberg extends App with IcebergSupport {
  private val uuid = UUID.randomUUID()

  val spark = sparkCatalogBuilder
    .config("spark.wap.branch", "feature_branch")
    .getOrCreate()

  private val tableName = "iceberg.finance.activity2"
  spark.sql(s"drop table if exists $tableName")

  def loadForDate(eodDate: String): Unit = {
    println(s"Process EOD: ${eodDate}")
    println("-----------------------")
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

    val beforeCount = spark.sql(s"select * from $tableName").count()
    println(s"data row count: ${beforeCount}")

    spark.read
      .format("csv")
      .option("header", "true")
      .schema(StructType(Seq[StructField](
        StructField("account", StringType),
        StructField("txn_date", DateType),
        StructField("txn_id", StringType),
        StructField("merchant", StringType),
        StructField("amount", DoubleType),
        StructField("category", StringType),
        StructField("last_updated", TimestampType)
      )))
      .load(s"src/main/resources/input/$eodDate")
      .writeTo(tableName).append()

    val auditCount = spark.sql(s"select * from $tableName").count()
    println(s"data row count: ${auditCount}")

    val branchDf = spark.sql(s"select * from $tableName version as of 'feature_branch'")
    val invalidData = branchDf.filter(row => (0 to row.length - 1).exists(idx => {
      if (row.isNullAt(idx)) {
        println(s"ERROR: column: ${idx} is null in ${row}")
      }
      row.isNullAt(idx)
    })).count()
    println(s"error count: ${invalidData}")
    if (invalidData > 0) {
      println("deleting data because of invalid data")
    } else {
      println("fast forwarding change from audit branch to main")
      spark.sql(s"call iceberg.system.fast_forward('$tableName', 'main', 'feature_branch')")
    }
    println("cleaning up feature branch")
    spark.sql(s"alter table $tableName drop branch feature_branch")
    val dataCount = spark.sql(s"select * from $tableName").count()
    println(s"Data Count after load ${dataCount}")
  }


  private val eodDate = "2024-03-02"
  loadForDate("2024-03-02")
  loadForDate("2024-03-10")

  //account,txn_date,txn_id,merchant,amount,category,last_updated


  spark.close()

}
