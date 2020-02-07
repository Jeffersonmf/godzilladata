package core

import com.amazonaws.regions.{Region, Regions}
import config.Environment
import exceptions.LoadDataException
import history._
import org.apache.spark.sql.functions.{lit, max, row_number}
import org.apache.spark.sql
import org.apache.spark.sql._
import org.apache.spark.sql.types.StringType
import org.joda.time.DateTime
import utils.Utils

object EnrichmentEngine  {

  val spark = sparkContextInitialize()
  spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", Environment.aws_access_key())
  spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", Environment.aws_secret_key())

  private def sparkContextInitialize(): SparkSession = {
    SparkSession.builder
      .master("local")
      .appName("SwapBigdataLegacy")
      .config("spark.some.config.option", true).getOrCreate()
  }

  private def updateHistoryOfExecution(processedFiles: Seq[HistoryStackFile]) = {
    HistoryOfExecutions.updateHistoryOfExecution(processedFiles, spark)
  }

  def chargeLocalSourceData(pathComplement: String): Boolean = {
    if (Utils.isEmptyOrNull(pathComplement))
      return false

    this.processInLocalStorage(pathComplement)
  }

  def chargeS3Bucket(bucketName: String, prefix: String): Boolean = {
    if (Utils.isEmptyOrNull(bucketName))
      return false

    this.processInAWS(bucketName, prefix)
  }

  private def processInAWS(bucketName: String, prefix: String): Boolean = {
    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._
    var processStatus: Boolean = false
    val destPath = Environment.getParquetDestinationFolder(Environment.isRunningLocalMode())
    val Swap_logDNA_Region = Region.getRegion(Regions.US_EAST_1)

    try {
      val filesToProcess = HistoryOfExecutions.checkHistoryOfExecution(Utils.getListFiles(bucketName, prefix, Swap_logDNA_Region), spark)
      //      val filesToProcess = Seq("s3a://swap-log-dna/2019/11/8fa4fbf955.2019-11-01.72.ld72.json.gz",
      //      "s3a://swap-log-dna/2019/11/8fa4fbf955.2019-11-02.72.ld72.json.gz")
      if (filesToProcess.size > 0) {
        val df = spark.read.json(filesToProcess: _*)
        df.withColumn("_source._ts", df.col("_source._ts").cast(sql.types.LongType))
        df.withColumn("_source._host", df.col("_source._host"))
        df.createOrReplaceTempView("dataFrame")

        val dftemp = spark.sql("SELECT _source._host as host, " +
          "_source._logtype as logtype, " +
          "_source._mac as mac, " +
          "_source._file as file, " +
          "_source._line as line, " +
          "_source._ts as ts, " +
          "from_unixtime(_source._ts / 1000, \"yyyy-MM-dd HH:mm:ss.SSSS\") as datetime, " +
          "_source._app as app," +
          "_source._ip as ip," +
          "_source._ipremote as ipremote," +
          "_source.level as level," +
          "_source._lid as lid" +
          " FROM dataFrame")
          .withColumn("account", lit(null).cast(StringType))
          .toDF()
          .write.mode(SaveMode.Append)
          .parquet(destPath)

        def addToHistory =
          for {
            fileName <- filesToProcess
          } yield HistoryStackFile(appName = bucketName,
            fileSourceName = fileName.toString(),
            timestamp = DateTime.now().getMillis,
            dateTime = DateTime.now().toString)

        updateHistoryOfExecution(addToHistory)
      }

      processStatus = true
    } catch {
      case e: Exception => {
        throw new LoadDataException("Retorno do Processamento.: ".concat(false.toString).concat(" \n\nProblema no enriquecimento dos dados puros... Detalhes:".concat(e.getMessage)))
      }
    }
    processStatus
  }

  private def processInLocalStorage(path: String): Boolean = {
    val sourcePath = Environment.getSourceFolder(Environment.isRunningLocalMode()).concat(if (path != null) path else "")
    processAndChargeData(sourcePath)
  }

  private def processAndChargeData(path: String): Boolean = {
    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._
    var processStatus: Boolean = false
    val destPath = Environment.getParquetDestinationFolder(Environment.isRunningLocalMode())

    try {
      val files: Seq[String] = Utils.getListLocalFiles(path)

      val df = spark.sqlContext.read.json(path);
      df.withColumn("_source._ts", df.col("_source._ts").cast(sql.types.LongType))
      df.withColumn("_source._host", df.col("_source._host"))
      df.createOrReplaceTempView("dataFrame")

      val dftemp = spark.sql("SELECT _source._host as host, " +
        "_source._logtype as logtype, " +
        "_source._mac as mac, " +
        "_source._file as file, " +
        "_source._line as line, " +
        "_source._ts as ts, " +
        "from_unixtime(_source._ts / 1000, \"yyyy-MM-dd HH:mm:ss.SSSS\") as datetime, " +
        "_source._app as app," +
        "_source._ip as ip," +
        "_source._ipremote as ipremote," +
        "_source.level as level," +
        "_source._lid as lid" +
        " FROM dataFrame")
      //                  .withColumn("account", lit(null).cast(StringType))
      //                  .toDF()
      //                  .write.mode(SaveMode.Append)
      //                  //.partitionBy("logtype")
      //                  .parquet(destPath)

      processStatus
    } catch {
      case e: Exception => {
        throw new LoadDataException("Retorno do Processamento.: ".concat(false.toString).concat(" \n\nProblema no enriquecimento dos dados puros... Detalhes:".concat(e.getMessage)))
      }
    }
  }

  def deleteSourceData(complementPath: String): Boolean = {
    var processStatus: Boolean = false
    val destPath = Environment.getParquetDestinationFolder(Environment.isRunningLocalMode())

    try {

      val df = spark.read.format("parquet")
        .load(destPath)
        .createTempView("logdna_datalake")

      val dftemp = spark.sql("SELECT * FROM logdna_datalake where datetime not like '%2019-11%'").toDF()
        .write.mode(SaveMode.Overwrite)
        .parquet("s3a://swap-bigdata/legacy/logdna_clean.parquet")

      processStatus = true

    } catch {
      case e: Exception => {
        throw new LoadDataException("Retorno do Processamento.: ".concat(false.toString).concat(" \n\nProblema no enriquecimento dos dados puros... Detalhes:".concat(e.getMessage)))
      }
    } finally {
      processStatus
    }

    processStatus
  }
}