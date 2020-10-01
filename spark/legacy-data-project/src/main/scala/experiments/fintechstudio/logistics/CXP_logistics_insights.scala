package experiments.fintechstudio.logistics

import com.amazonaws.regions.{Region, Regions}
import config.Environment
import contracts.SchemaContracts
import core.SwapCoreBase
import exceptions.LoadDataException
import org.apache.spark.sql
import org.apache.spark.sql._
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{DataType, Decimal, DecimalType, DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{SaveMode, SparkSession}
import utils.Utils

object CXP_logistics_insights extends SwapCoreBase {

  def QUERY_TO_CHARGER_EXTRACT_PAGCORP = {
      "SELECT Centro_Custo, Data_Cod, Data_Orig, Id_Conta, Cod_Transacao, Tipo_Transacao, Descricao, Historico, ValorCredito, ValorDebito, ValorSaldo, ValorDestino FROM table_extract_pagcorp where id_conta <> 'Total Geral'"
  }

  def sparkSession = {
    super.getSparkContext()
  }

  def extractTableSchema = {
    StructType(Seq(
      StructField("request_id", StringType, true),
      StructField("datetime", StringType, true),
      StructField("account_id", StringType, true),
      StructField("uri", StringType, true),
      StructField("line", StringType, true)))
  }

  override def main(args: Array[String]): Unit = {
    if (args.nonEmpty && args.size > 2) {
      val sparkSession = super.getSparkContext()

      var processname = args(0)
      var bucketname = args(1)
      var prefixFilter = args(2)

      processname match {
        case "flash-extract-pagcorp" => charger_extract_pagcorp(bucketname, prefixFilter, sparkSession)
        case _ => argsException
      }
    } else {
      argsException
    }
  }

  def argsException = {
    throw new Exception("Argumentos inválidos para execução.  \n\n Os argumentos esperados são: arg1 = processname [flash-orders], arg2 = BucketName, arg3 = PrefixFilter [csv/flash-orders]")
  }

  private def charger_extract_pagcorp(bucketName: String, prefix: String, sparkSession: SparkSession): Boolean = {
    import sparkSession.implicits._

    import DFHelper._

    var processStatus: Boolean = false
    val destPath = Environment.getFlashExtractPagcorpDestination(Environment.isRunningLocalMode())
    val sourcePath = Environment.getFlashExtractPagcorpSource(Environment.isRunningLocalMode())
    val Swap_Customer_Experience_Region = Region.getRegion(Regions.US_EAST_2)

    try {
      val filesToProcess = Utils.getListFiles(bucketName, prefix, Swap_Customer_Experience_Region).drop(1)

      if (filesToProcess.size > 0) {
        val df = sparkSession.read.option("header", "true").option("sep", ";").csv(sourcePath)
        val df2 = df.withColumn("ValorCredito", df("ValorCredito").cast(org.apache.spark.sql.types.DoubleType))
        .withColumn("ValorDebito", df("ValorDebito").cast(DecimalType(38,0)))
        .withColumn("ValorSaldo", df("ValorSaldo").cast(DecimalType(38,0)))
        .withColumn("ValorDestino", df("ValorDestino").cast(DecimalType(38,0)))

        df.createOrReplaceTempView("table_extract_pagcorp")
        sparkSession.sql(QUERY_TO_CHARGER_EXTRACT_PAGCORP).toDF()
        .write.mode(SaveMode.Overwrite)
        .parquet(destPath)

        processStatus = true
      }
    } catch {
      case e: Exception => {
        throw new LoadDataException("Retorno de Processamento no Spark.: ".concat(false.toString).concat(" \n\nDetalhes:".concat(e.getMessage)))
      }
    }
    processStatus
  }

  def resetOrdersSourceData(sparkSession: SparkSession): Boolean = {
    var processStatus: Boolean = false
    val destPath = Environment.getFlashExtractPagcorpDestination(Environment.isRunningLocalMode())

    try {
      sparkSession.read.format("parquet").schema(SchemaContracts.ordersFlashSchema)
        .load(destPath)
        .createTempView("table_extract_pagcorp")

      sparkSession.sql("select * from table_extract_pagcorp where false").toDF()
        .write.mode(SaveMode.Overwrite)
        .parquet(destPath)
    } catch {
      case e: Exception => {
        throw new LoadDataException("Retorno do Processamento.: ".concat(false.toString).concat(" \n\nProblema no enriquecimento dos dados puros... Detalhes:".concat(e.getMessage)))
      }
    } finally {
      processStatus = true
    }
    processStatus
  }
}

object DFHelper{
  def castColumnTo( df: DataFrame, cn: String, tpe: DataType ) : DataFrame = {
    df.withColumn( cn, df(cn).cast(tpe) )
  }
}