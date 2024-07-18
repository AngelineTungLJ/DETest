import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite
import com.example.RDD_2

class MainTest extends AnyFunSuite {

  val conf = new SparkConf().setAppName("Spark RDD Example Test").setMaster("local[*]")
  val sc = new SparkContext(conf)
  val spark = SparkSession.builder().appName("Spark RDD Example Test").getOrCreate()

  val schemaA = StructType(Array(
    StructField("geographical_location_oid", LongType, nullable = false),
    StructField("video_camera_oid", LongType, nullable = false),
    StructField("detection_oid", LongType, nullable = false),
    StructField("item_name", StringType, nullable = false),
    StructField("timestamp_detected", LongType, nullable = false)
  ))

  val schemaB = StructType(Array(
    StructField("geographical_location_oid", LongType, nullable = false),
    StructField("geographical_location", StringType, nullable = false)
  ))

  test("RDD transformation and joining work correctly") {
    // Sample data
    val dataA = Seq(
      Row(1L, 101L, 1001L, "item1", 1627891200L),
      Row(2L, 102L, 1002L, "item2", 1627891201L),
      Row(3L, 103L, 1003L, "item3", 1627891202L)
    )

    val dataB = Seq(
      Row(1L, "location1"),
      Row(2L, "location2")
    )

    // Create DataFrames from sample data
    val rddA = sc.parallelize(dataA)
    val rddB = sc.parallelize(dataB)

    val dfA = spark.createDataFrame(rddA, schemaA)
    val dfB = spark.createDataFrame(rddB, schemaB)

    // Convert DataFrames to RDDs
    val rddARow = dfA.rdd
    val rddBRow = dfB.rdd

    // Map and join RDDs
    val rddAMapped = rddARow.map(row => (row.getAs[Long]("geographical_location_oid"), row))
    val rddBMapped = rddBRow.map(row => (row.getAs[Long]("geographical_location_oid"), row.getAs[String]("geographical_location")))

    val leftJoinRDD = RDD_2.combine2RDDs(rddAMapped, rddBMapped, numSalts = 10, applySalt = false)

    val expected = Seq(
      Row(1L, 101L, 1001L, "item1", 1627891200L, "location1"),
      Row(2L, 102L, 1002L, "item2", 1627891201L, "location2"),
      Row(3L, 103L, 1003L, "item3", 1627891202L, null)
    )

    assert(leftJoinRDD.collect().toSeq === expected)

    // Stop SparkContext
    sc.stop()
  }
}
