package history

import java.sql.Date

import config.Environment
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.col
import org.joda.time.DateTime

case class ProcessedStackHistory(appName: String, fileSourceName: String, dateTime: DateTime)

object HistoryOfExecutions {

  def updateHistoryOfExecution(historyList: List[ProcessedStackHistory], sc: SparkContext, spark: SparkSession): Unit = {
    import spark.implicits._
  }

  def checkHistoryOfExecution(historyList: List[ProcessedStackHistory], sc: SparkContext, spark: SparkSession): List[String] = {
    import spark.implicits._

    val historyInParquet = spark.read.parquet(Environment.getHistoryDestinationFolder(Environment.isRunningAWSMode()))

    Nil
  }
}