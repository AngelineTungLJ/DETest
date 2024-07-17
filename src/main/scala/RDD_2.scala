package com.example

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import scala.util.{Try, Success, Failure}


object RDD_2 {
  def readParquetFile(sc: SparkContext, spark: SparkSession, filePath: String, schema: StructType): Try[RDD[Row]] = {
    Try {
      spark.read.schema(schema).parquet(filePath).rdd
    }
  }

  def addSalt(row: Row, numSalts: Int): Seq[(Long, Row)] = {
    val locationId = row.getAs[Long]("geographical_location_oid")
    (0 until numSalts).map(salt => (locationId * numSalts + salt, row))
  }

  def combine2RDDs(rddAMapped: RDD[(Long, Row)], rddBMapped: RDD[(Long, String)], numSalts: Int, applySalt: Boolean): RDD[Row] = {
    val rddASalted = if (applySalt) {
      rddAMapped.flatMap { case (key, row) => addSalt(row, numSalts) }
    } else {
      rddAMapped.map { case (key, row) => (key, row) }
    }

    val rddBSalted = if (applySalt) {
      rddBMapped.flatMap { case (key, value) => (0 until numSalts).map(salt => (key * numSalts + salt, value)) }
    } else {
      rddBMapped.map { case (key, value) => (key, value) }
    }

    val rddAMappedDistinct = rddASalted.distinct()

    val matchedPairs = rddAMappedDistinct
      .join(rddBSalted)
      .map { case (_, (rowA, geoLocation)) =>
        Row(
          rowA.getAs[Long]("geographical_location_oid"),
          rowA.getAs[Long]("video_camera_oid"),
          rowA.getAs[Long]("detection_oid"),
          rowA.getAs[String]("item_name"),
          rowA.getAs[Long]("timestamp_detected"),
          geoLocation
        )
      }

    val unmatchedPairs = rddAMappedDistinct
      .leftOuterJoin(rddBSalted)
      .filter { case (_, (_, geoLocation)) => geoLocation.isEmpty }
      .map { case (_, (rowA, _)) =>
        Row(
          rowA.getAs[Long]("geographical_location_oid"),
          rowA.getAs[Long]("video_camera_oid"),
          rowA.getAs[Long]("detection_oid"),
          rowA.getAs[String]("item_name"),
          rowA.getAs[Long]("timestamp_detected"),
          null
        )
      }

    matchedPairs.union(unmatchedPairs)
  }

  def findTopXItemsPerLocation(sc: SparkContext, spark: SparkSession, filePath: String, x: Int, outputFilePath: String): Try[Unit] = {
    readParquetFile(sc, spark, filePath, schema).flatMap { rdd =>
      Try {
        val locationItemPairs = rdd.map(row => (
          row.getAs[Long]("geographical_location_oid"),
          row.getAs[String]("item_name")
        ))

        val locationItemCounts = locationItemPairs
          .map { case (location, item) => ((location, item), 1) }
          .reduceByKey(_ + _)
          .map { case ((location, item), count) => (location, (item, count)) }

        val topXItemsPerLocation = locationItemCounts
          .groupByKey()
          .flatMap { case (location, items) =>
            items.toList.sortBy(-_._2).take(x).zipWithIndex.map { case ((item, _), rank) =>
              Row(location, rank + 1, item)
            }
          }

        val schemaOut = StructType(Array(
          StructField("geographical_location_oid", LongType, nullable = false),
          StructField("item_rank", IntegerType, nullable = false),
          StructField("item_name", StringType, nullable = false)
        ))

        val resultDF = spark.createDataFrame(topXItemsPerLocation, schemaOut)
        resultDF.write.mode("overwrite").parquet(outputFilePath)
      }
    }
  }

  private def schema: StructType = StructType(Array(
    StructField("geographical_location_oid", LongType, nullable = false),
    StructField("item_name", StringType, nullable = false)
  ))
}