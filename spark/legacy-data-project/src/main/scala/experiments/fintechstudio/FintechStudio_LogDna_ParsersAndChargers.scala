package experiments.fintechstudio

import com.amazonaws.regions.{Region, Regions}
import config.Environment
import core.EnrichmentEngine.{spark, updateHistoryOfExecution}
import core.SwapCoreBase
import exceptions.LoadDataException
import history.{HistoryOfExecutions, HistoryStackFile}
import org.apache.spark.sql
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.joda.time.DateTime
import utils.Utils

object FintechStudio_LogDna_ParsersAndChargers extends SwapCoreBase {

  def regex_requestID = {
    """request_id=[^\s]*""".r
  }

  def regex_line = {
    """line=[^\s]*""".r
  }

  def regex_accountID = {
    """account_id" => "[^\"]+""".r
  }

  def regex_URI = {
    """\{"URI", "[^\"]+""".r
  }

  def smaugRequestTableSchema = {
    StructType(Seq(
      StructField("request_id", StringType, true),
      StructField("datetime", StringType, true),
      StructField("account_id", StringType, true),
      StructField("uri", StringType, true),
      StructField("line", StringType, true)))
  }

  case class SmaugRequestSchema(request_id: String, datetime: String, account_id: String, uri: String, line: String)
  case class LegacyLogDNA(host: String, logtype: String, mac: String, file: String, line: String, ts: Long, datetime: String, app: String, ip: String, ipremote: String, level: String, lid: String, account: String)

  override def main(args: Array[String]): Unit = {
    if (args.nonEmpty && args.size > 0) {
      val sparkSession = super.getSparkContext()

      val processname = args(0)
      val bucketname = if(args(1)!=null) args(1) else ""
      val prefixFilter = if(args(2)!=null) args(2) else ""
      val partedby = if(args(3)!=null) args(3) else ""

      processname match {
        case "fintech-requestparser" => parserLogDNARequests(sparkSession)
        case "fintech-charger-logdna-partedby" => chargerLogDNAPartedBy(bucketname, prefixFilter, partedby, sparkSession)
        case _ => argsException
      }
    } else {
      argsException
    }
  }

  def argsException = {
    throw new Exception("Argumentos inválidos para execução.  \n\n Os argumentos esperados são: arg1 = processname [fintech-requestparser]")
  }

  private def parseLine(line: String): Row = {
    try{
      val date = line.substring(0, 12)
      val requestID = regex_requestID.findFirstIn(line).getOrElse("").toString.replaceAll("""request_id=""", """""")
      val lineInLine = regex_line.findFirstIn(line).getOrElse("0").toString
      val accountID = regex_accountID.findFirstIn(line).getOrElse("").toString.replaceAll("""account_id\" => \"""", """""")
      val uri = regex_URI.findFirstIn(line).getOrElse("").toString.replaceAll("""\"\{\"URI\",\"""", """""")

      Row(requestID, date, accountID, uri, lineInLine)
    } catch {
      case e: Exception => Row("ERRO", "", "", "", "")
    }

  }

  private def parserLogDNARequests(sparkSession: SparkSession): Boolean = {
    import org.apache.spark.sql.{Row, _}
    import org.apache.spark.sql.functions.{col, concat, datediff, lit, substring, when}
    import sparkSession.implicits._
    import org.joda.time.DateTimeZone

    import java.sql.Timestamp
    import java.time.Instant
    import org.joda.time.format.DateTimeFormat

    DateTimeZone.setDefault(DateTimeZone.UTC)

    val formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.sss")
    val dt = formatter.parseDateTime("2020-02-29 23:59:59.59")

    val timestamp = new Timestamp(dt.getMillis()) : Timestamp
    // To get the epoch timestamp, take a look at java.time.Instant
    val epochTimestamp =  timestamp.toInstant() : Instant

    def QUERY_TO_PARSER_LOGDNA_FLAT = {
      "select CAST(from_unixtime(CAST(ts AS bigint)/1000) AS date) as dateX, line from SMAUG_FilteredTransformation where CAST(from_unixtime(CAST(ts AS bigint)/1000) AS date) between '2020-02-01' and '2020-02-29' order by dateX desc"
    }

    var processStatus: Boolean = false
    val destPath = Environment.getRequestIDParserDestination(Environment.isRunningLocalMode())
    val sourcePath = Environment.getRequestIDParserSource(Environment.isRunningLocalMode())
    val Swap_Customer_Experience_Region = Region.getRegion(Regions.US_EAST_2)

    try {
//      val df = sparkSession.read.format("parquet")
//        .load(sourcePath)select(col("line"))

      val df2 = sparkSession.read.format("parquet")
        .load(sourcePath).select(col("line"), col("ts")).filter((col("date").equalTo("2020-02-01")))
      df2.createOrReplaceTempView("SMAUG_FilteredTransformation")

      //sparkSession.sql(QUERY_TO_PARSER_LOGDNA_FLAT).collect()
      //val x = df2.

      var sourceList = Seq[Row]()
//
//      var i = 0
//
//      for(item <- x) {
//        i += 1
//      }

      df2.take(df2.count.toInt).foreach(t =>
        sourceList = sourceList :+ parseLine(t.getString(0))
      )

      //sparkSession.sqlContext.clearCache()

      val rdd = sparkSession.sparkContext.parallelize(sourceList)
      val dfFinal = sparkSession.createDataFrame(rdd, smaugRequestTableSchema)
              .write.mode(SaveMode.Overwrite)
              .parquet(destPath)

      //      dfFinal.createOrReplaceTempView("SMAUG_FilteredTransformation")
//
//      sparkSession.sqlContext.setConf("spark.driver.maxResultSize","20g")
//      sparkSession.sql(QUERY_TO_PARSER_LOGDNA_FLAT)
//        //          .schema(smaugRequestTableSchema.fieldNames)
//        .write.mode(SaveMode.Overwrite)
//        .parquet(destPath)

      processStatus = true
    } catch {
      case e: Exception => {
        throw new LoadDataException("Retorno do Processamento.: ".concat(false.toString).concat(" \n\n Detalhes:".concat(e.getMessage)))
      }
    }
    processStatus
  }


  private def chargerLogDNAPartedBy(bucketName: String, prefix: String, partedbyColumn: String, sparkSession: SparkSession): Boolean = {
    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._
    var processStatus: Boolean = false

    def matchDestPath(partedbyColumn: String): String = partedbyColumn match {
      case "logtype" => Environment.getLogDnaByLogtypeDestinationFolder(Environment.isRunningLocalMode())
      case "date" => Environment.getLogDnaByDateDestinationFolder(Environment.isRunningLocalMode())
      case _ => Environment.getLogDnaByLogtypeDestinationFolder(Environment.isRunningLocalMode())
    }

    val destPath = matchDestPath(partedbyColumn)

    val Swap_logDNA_Region = Region.getRegion(Regions.US_EAST_1)

    try {
      val filesToProcess = Utils.getListFiles(bucketName, prefix, Swap_logDNA_Region)
      //      val filesToProcess = Seq("s3a://swap-log-dna/2019/11/8fa4fbf955.2019-11-01.72.ld72.json.gz",
      //      "s3a://swap-log-dna/2019/11/8fa4fbf955.2019-11-02.72.ld72.json.gz")
      if (filesToProcess.size > 0) {
        val df = sparkSession.read.json(filesToProcess: _*)
        df.withColumn("_source._ts", df.col("_source._ts").cast(sql.types.LongType))
        df.withColumn("_source._host", df.col("_source._host"))
        df.createOrReplaceTempView("dataFrame")

        val dftemp = sparkSession.sql("SELECT _source._host as host, " +
          "_source._logtype as logtype, " +
          "_source._mac as mac, " +
          "_source._file as file, " +
          "_source._line as line, " +
          "_source._ts as ts, " +
          "from_unixtime(_source._ts / 1000, \"yyyy-MM-dd HH:mm:ss.SSSS\") as datetime, " +
          "from_unixtime(_source._ts / 1000, \"yyyy-MM-dd\") as date, " +
          "_source._app as app," +
          "_source._ip as ip," +
          "_source._ipremote as ipremote," +
          "_source.level as level," +
          "_source._lid as lid" +
          " FROM dataFrame")
          .withColumn("account", lit(null).cast(StringType))
          .toDF()
          .write.mode(SaveMode.Overwrite)
          .partitionBy(partedbyColumn)
          .parquet(destPath)
      }

      processStatus = true
    } catch {
      case e: Exception => {
        processStatus = false
        throw new LoadDataException("Retorno do Processamento.: ".concat(false.toString).concat(" \n\nProblema no enriquecimento dos dados puros... Detalhes:".concat(e.getMessage)))
      }
    }
    processStatus
  }

}