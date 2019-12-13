package core

import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.auth.BasicAWSCredentials
import config.Environment
import exceptions.LoadDataException
import org.apache.spark.sql.{SaveMode, SparkSession}
import java.io.File
import utils.Utils

object EnrichmentEngine {

  val spark = sparkContextInitialize()

  private def sparkContextInitialize(): SparkSession = {
    SparkSession.builder
      .master("local")
      .appName("SwapBigdataLegacy")
      .config("spark.some.config.option", true).getOrCreate()
  }

  private def updateHistoryOfExecution(enrichmentFiles: List[File]) = {
    //TODO: Calls here the history modules.
  }

  def chargeSourceData(pathComplement: String): Boolean = {

    var processStatus: Boolean = false

    if (Utils.isEmptyOrNull(pathComplement))
      return false

      Environment.isRunningAWSMode() match {
        case false => processStatus = this.processInLocalStorage(pathComplement)
        case true => processStatus = this.processInAWS(pathComplement)
      }

    processStatus
  }

  private def processInAWS(path: String): Boolean = {

    var processStatus: Boolean = false
    //TODO: Include here all logic to process directly on AWS S3...
    spark.sparkContext.hadoopConfiguration.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", Environment.aws_access_key())
    spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", Environment.aws_access_key())

    val sourcePath = Environment.get_AWS_JsonSourceFolder().concat(if (path != null) path else "")
    val destPath = Environment.get_AWS_ParquetDestinationFolder()

    val enrichmentFiles = Utils.getListOfFiles(sourcePath)

//      val bucketName = "swap-bigdata"
//
//      //file to upload
//      val fileToUpload = new File("/home/neelkanth/Desktop/Neelkanth_Sachdeva.png")
//
//      val yourAWSCredentials = new BasicAWSCredentials(Environment.aws_access_key(), Environment.aws_secret_key())
//      val amazonS3Client = new AmazonS3Client(yourAWSCredentials)
//      // This will create a bucket for storage
//      amazonS3Client.putObject(bucketName, "FileName", fileToUpload)

      try {
      if (!Utils.isSourceFolderEmpty(sourcePath)) {
        val df = spark.read.format("json").json(sourcePath)
//        spark.read.parquet("s3n://bucket/abc.parquet")    //Read
//        df.rdd.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", Environment.aws_access_key())
//        df.rdd.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", Environment.aws_secret_key())
//        df.rdd.sparkContext.hadoopConfiguration.set("fs.s3.access.key", Environment.aws_access_key())
//        df.rdd.sparkContext.hadoopConfiguration.set("fs.s3.secret.key", Environment.aws_secret_key())
//        df.rdd.sparkContext.hadoopConfiguration.set("fs.s3n.access.key", Environment.aws_access_key())
//        df.rdd.sparkContext.hadoopConfiguration.set("fs.s3n.secret.key", Environment.aws_secret_key())
//        spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "s3.ap-south-1.amazonaws.com")
        spark.sparkContext.hadoopConfiguration.set("fs.s3a.awsAccessKeyId", Environment.aws_access_key())
        spark.sparkContext.hadoopConfiguration.set("fs.s3a.awsSecretAccessKey", Environment.aws_secret_key())
        df.rdd.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", Environment.aws_access_key())
        df.rdd.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", Environment.aws_secret_key())

        df.toDF()
          .write
          .mode(SaveMode.Append)
          .parquet("s3a://" + Environment.aws_access_key() + ":" + Environment.aws_secret_key() + "@swap-bigdata/legacy/logdna.parquet")    //Write
          //.parquet("s3a://swap-bigdata/legacy/")    //Write


//        df.toDF()
//          .write
//          .mode(SaveMode.Append)
//          .parquet(destPath)

        processStatus = true
      }
    } catch {
      case e: Exception => {
        throw e
        //throw new LoadDataException("Retorno do Processamento.: ".concat(false.toString).concat(" \n\nProblema no enriquecimento dos dados puros... Detalhes:".concat(e.getMessage)))
      }
    } finally {

      if (processStatus)
        updateHistoryOfExecution(enrichmentFiles)
    }

    false
  }

  private def processInLocalStorage(path: String): Boolean = {
    var processStatus: Boolean = false

    val sourcePath = Environment.getJsonSourceFolder().concat(if (path != null) path else "")
    val destPath = Environment.getParquetDestinationFolder()

    val enrichmentFiles = Utils.getListOfFiles(sourcePath)

    try {
      if (!Utils.isSourceFolderEmpty(sourcePath)) {
        val df = spark.read.format("json").json(sourcePath)

        df.rdd.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", Environment.aws_access_key())
        df.rdd.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", Environment.aws_secret_key())

        df.toDF()
          .write
          .mode(SaveMode.Append)
          .parquet(destPath)

        processStatus = true
      }
    } catch {
      case e: Exception => {
        throw e
        //throw new LoadDataException("Retorno do Processamento.: ".concat(false.toString).concat(" \n\nProblema no enriquecimento dos dados puros... Detalhes:".concat(e.getMessage)))
      }
    } finally {

      if (processStatus)
        updateHistoryOfExecution(enrichmentFiles)
    }

    processStatus
  }
}