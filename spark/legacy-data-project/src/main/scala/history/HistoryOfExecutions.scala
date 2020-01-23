package history

import config.Environment
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Encoders, SaveMode, SparkSession}
import org.joda.time.DateTime

case class HistoryStackFile(appName: String, fileSourceName: String, timestamp: Long, dateTime: String)

object HistoryOfExecutions extends App {

  private def sparkContextInitialize(): SparkSession = {
    SparkSession.builder
      .master("local")
      .appName("SwapBigdataLegacy")
      .config("spark.some.config.option", true).getOrCreate()
  }

  override def main(args: Array[String]): Unit = {

    val spark = sparkContextInitialize()
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", Environment.aws_access_key())
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", Environment.aws_secret_key())

    val historyList = Seq("8fa4fbf955.2019-11-04.72.ld72.json.gz", "8fa4fbf955.2019-11-03.72.ld72.json.gz"
      ,"ABC123456", "ZXF232322323232")
    val zzz = this.checkHistoryOfExecution(historyList, spark)

  }

  def updateHistoryOfExecution(addToStackHistory: Seq[HistoryStackFile], spark: SparkSession): Unit = {
    import spark.implicits._

    addToStackHistory.toDF()
      .write.mode(SaveMode.Append)
      .partitionBy("appName")
      .parquet(Environment.getHistoryDestinationFolder(Environment.isRunningLocalMode()))
  }

  def checkHistoryOfExecution(filesToStackProcess: Seq[String], spark: SparkSession): Seq[String] = {
    import spark.implicits._

    try {
      val historyInParquet = spark.read.format("parquet").parquet(Environment.getHistoryDestinationFolder(Environment.isRunningLocalMode()))
      val filesToAddIntoStack = filesToStackProcess.toDS().toDF()

      val result = historyInParquet.select("fileSourceName")
        .intersect(filesToAddIntoStack.select("value"))

      val itemsToRemove = result.as(Encoders.STRING).collectAsList

      def filesToProcess =
        for {
          fileName <- filesToStackProcess.filter(filename => !itemsToRemove.toArray()
            .toList.contains(filename))
        } yield fileName.toString()

      filesToProcess
    } catch {
      case e: Exception => {
        filesToStackProcess
      }
    }
  }
}