import com.example.RDD_2

import java.util.Properties
import java.io.FileInputStream
import scala.util.{Failure, Success, Try}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

class SparkApp(config: Properties) {
  val inputPath1: String = config.getProperty("inputPath1")
  val inputPath2: String = config.getProperty("inputPath2")
  val outputPath: String = config.getProperty("outputPath")
  val topX: Int = config.getProperty("topX").toInt
  val numSalts: Int = config.getProperty("numSalts", "10").toInt
  val applySalt: Boolean = config.getProperty("applySalt", "false").toBoolean

  val conf = new SparkConf().setAppName("Data Engineering").setMaster("local[*]")
  val sc = new SparkContext(conf)
  val spark = SparkSession.builder().appName("Data Engineering").getOrCreate()

  def run(): Unit = {
    val result = for {
      rddA <- RDD_2.readParquetFile(sc, spark, inputPath1, schemaA)
      rddB <- RDD_2.readParquetFile(sc, spark, inputPath2, schemaB)
    } yield {
      val rddAMapped = rddA.map(row => (row.getAs[Long]("geographical_location_oid"), row))
      val rddBMapped = rddB.map(row => (row.getAs[Long]("geographical_location_oid"), row.getAs[String]("geographical_location")))

      val leftJoinRDD = RDD_2.combine2RDDs(rddAMapped, rddBMapped, numSalts, applySalt)

      println("Contents of leftJoinRDD:")
      leftJoinRDD.collect().foreach(println)

      RDD_2.findTopXItemsPerLocation(sc, spark, inputPath1, topX, outputPath)

      val outputDF = spark.read.parquet(outputPath)
      outputDF.collect().foreach(println)
    }

    result match {
      case Success(_) => println("Processing completed successfully.")
      case Failure(e) => println(s"Error during processing: ${e.getMessage}")
    }
  }

  def stop(): Unit = {
    sc.stop()
  }

  private def schemaA: StructType = StructType(Array(
    StructField("geographical_location_oid", LongType, nullable = false),
    StructField("video_camera_oid", LongType, nullable = false),
    StructField("detection_oid", LongType, nullable = false),
    StructField("item_name", StringType, nullable = false),
    StructField("timestamp_detected", LongType, nullable = false)
  ))

  private def schemaB: StructType = StructType(Array(
    StructField("geographical_location_oid", LongType, nullable = false),
    StructField("geographical_location", StringType, nullable = false)
  ))
}

object Main {
  def main(args: Array[String]): Unit = {
    val config = new Properties()
    Try(config.load(new FileInputStream("config.properties"))) match {
      case Success(_) =>
        val app = new SparkApp(config)
        app.run()
        app.stop()
      case Failure(e) =>
        println(s"Error loading configuration: ${e.getMessage}")
    }
  }
}
