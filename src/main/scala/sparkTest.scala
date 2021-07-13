import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode.Append
import org.apache.spark.streaming.dstream.ConstantInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.io.File
import java.nio.file.Paths
import scala.util.{Random, Try}

object sparkTest {

  val currentDirFile = new File(".")
  val root: String = Paths.get(currentDirFile.toURI).getParent.toString

  def init(): SparkSession = {
    val warehouseLocation = new File(root + "/src/main/resources").getAbsolutePath

    val spark = SparkSession
      .builder()
      .appName("Spark Hive Example")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()

    spark
  }

  case class SensorData(sensorId: Int, timestamp: Long, value: Double)

  def main(args: Array[String]): Unit = {
    val spark = init()
    import spark.implicits._

    val sensorId: () => Int = () => Random.nextInt(100000)
    val data: () => Double = () => Random.nextDouble()
    val timestamp: () => Long = () => System.currentTimeMillis()
    val recordFunction: () => String = { () =>
      if (Random.nextDouble() < 0.9) {
        Seq(sensorId().toString, timestamp(), data()).mkString(",")
      } else {
        "!!~corrupt~^&##$"
      }
    }

    val sensorDataGenerator = spark.sparkContext.parallelize(1 to 100)
      .map(_ => recordFunction)
    val sensorData = sensorDataGenerator.map(recordFun => recordFun())

    val streamingContext = new StreamingContext(spark.sparkContext, Seconds(2))
    val rawDStream = new ConstantInputDStream(streamingContext, sensorData)

    val schemaStream = rawDStream.flatMap{record =>
      val fields = record.split(",")
      Try {
        SensorData(fields(0).toInt, fields(1).toLong, fields(2).toDouble)
      }.toOption
    }

    schemaStream.foreachRDD{ rdd =>
      val df = rdd.toDF()
      df.write.format("parquet").mode(Append).save("file://" + root + "/output/iotstream.parquet")
    }

    streamingContext.start()
    streamingContext.awaitTermination()

  }

}
