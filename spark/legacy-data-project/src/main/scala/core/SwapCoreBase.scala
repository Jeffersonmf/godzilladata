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
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", Environment.aws_access_key())
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", Environment.aws_secret_key())
    spark
  }

  def getClassNameSimplified: String = this.getClass.getSimpleName.replace("$", "")

  final case class DependencyException(private val message: String = "", private val cause: Throwable = None.orNull)
    extends Exception(message, cause)
}
