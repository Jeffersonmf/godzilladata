package core

import config.Environment
import org.apache.spark.sql.SparkSession

class SwapCoreBase() extends App {

  private def sparkContextInitialize(): SparkSession = {
    SparkSession.builder
      .master("local")
      .appName(getClassNameSimplified)
      .config("spark.some.config.option", true).getOrCreate()
  }

  def getSparkContext(): SparkSession = {
    val spark = sparkContextInitialize()
    spark.conf.set("spark.driver.memory", "16g")
    spark.conf.set("spark.executor.memory", "16g")
    spark.conf.set("spark.driver.maxResultSize","20g")
    spark.conf.set("spark.cores.max", "4")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", Environment.aws_access_key())
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", Environment.aws_secret_key())
    spark
  }

  def getClassNameSimplified: String = this.getClass.getSimpleName.replace("$", "")

  final case class DependencyException(private val message: String = "", private val cause: Throwable = None.orNull)
    extends Exception(message, cause)
}
