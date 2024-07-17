import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

import scala.util.Random

object CreateParquet {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Create Parquet File")
      .master("local[*]")
      .getOrCreate()

    // Define the original schema
    val originalSchema = StructType(Array(
      StructField("geographical_location_oid", LongType, nullable = false),
      StructField("video_camera_oid", LongType, nullable = false),
      StructField("detection_oid", LongType, nullable = false),
      StructField("item_name", StringType, nullable = false),
      StructField("timestamp_detected", LongType, nullable = false)
    ))

    // Define the new schema
    val newSchema = StructType(Array(
      StructField("geographical_location_oid", LongType, nullable = false),
      StructField("geographical_location", StringType, nullable = false)
    ))

    // Function to generate random data for the original schema
    def generateData(numRows: Int): Seq[Row] = {
      val items = Array("Cat", "Dog", "Person", "Car", "Bird")
      val rand = new Random()

      (1 to numRows).map { _ =>
        val geoLocId = rand.nextInt(10).toLong
        val videoCamId = rand.nextInt(10000).toLong
        val detectionId = rand.nextInt(10000).toLong
        val itemName = items(rand.nextInt(items.length))
        val timestampDetected = System.currentTimeMillis() - rand.nextInt(1000000000).toLong

        Row(geoLocId, videoCamId, detectionId, itemName, timestampDetected)
      }
    }

    // Function to generate random data for the new schema
    def generateNewData(existingGeoLocIds: Seq[Long]): Seq[Row] = {
      val locations = Array("Joo Avenue", "Joo Chiat Avenue", "Joo Chiat Lane", "Joo Chiat Place",
        "Joo Chiat Road", "Joo Chiat Terrace", "Joo Chiat Walk", "Joo Hong Road",
        "Joo Koon Circle", "Joo Koon Crescent", "Joo Koon Road", "Joo Koon Way",
        "Joo Seng Road", "Joo Yee Road", "Joon Hiang Road", "Jubilee Road",
        "Jupiter Road")

      val rand = new Random()

      existingGeoLocIds.map { geoLocId =>
        val geoLocation = locations(rand.nextInt(locations.length))
        Row(geoLocId, geoLocation)
      }
    }

    // Generate data for the original schema
    val originalData = generateData(1000)

    // Extract geographical_location_oid values from the original data
    val geoLocIds = originalData.map(row => row.getLong(0))

    // Generate data for the new schema
    val newData = generateNewData(geoLocIds)

    // Create the DataFrames
    val originalDF = spark.createDataFrame(
      spark.sparkContext.parallelize(originalData),
      originalSchema
    )

    val newDF = spark.createDataFrame(
      spark.sparkContext.parallelize(newData),
      newSchema
    )

    // Show the DataFrames
    originalDF.show()
    newDF.show()

    // Write the DataFrames to Parquet files
    originalDF.write.mode("overwrite").parquet("src/main/data/DatasetASample.parquet")
    newDF.write.mode("overwrite").parquet("src/main/data/DatasetBSample.parquet")

    spark.stop()
  }

}
