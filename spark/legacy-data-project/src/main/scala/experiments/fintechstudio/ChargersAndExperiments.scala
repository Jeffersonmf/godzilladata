package experiments.fintechstudio

import config.Environment
import core.EnrichmentEngine.spark
import core.SwapCoreBase
import exceptions.LoadDataException
import org.apache.spark.sql
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.StringType
import utils.Utils

object ChargersAndExperiments extends SwapCoreBase {

  override def main(args: Array[String]): Unit = {
    val sparkContext = super.getSparkContext()
    val experimentName = getClassNameSimplified

    val x = ""
  }

  private def enrichOrdersFlashCards(bucketName: String, prefix: String): Boolean = {
    // For implicit conversions like converting RDDs to DataFrames
    var processStatus: Boolean = false
    val destPath = Environment.getFlashOrdersDestination(Environment.isRunningLocalMode())

    try {
      val filesToProcess = Utils.getListFiles(bucketName, prefix)

      if (filesToProcess.size > 0) {
        val df = spark.read.json(filesToProcess: _*)
        df.createOrReplaceTempView("dataFrame")

        //flex	food	life	go	endereco	bairro	cep	cidade	estado	destinatario


        //TODO: Move this to the template file..
        val dftemp = spark.sql("SELECT orderId, companyId, empresa, cnpj, " +
          "from_unixtime(_source._ts / 1000, \"yyyy-MM-dd HH:mm:ss.SSSS\") as datetime, " +
          " FROM dataFrame")
          .withColumn("account", lit(null).cast(StringType))
          .toDF()
          .write.mode(SaveMode.Overwrite)
          .parquet(destPath)
      }
      processStatus = true
    } catch {
      case e: Exception => {
        throw new LoadDataException("Retorno do Processamento.: ".concat(false.toString).concat(" \n\nProblema no enriquecimento dos dados puros... Detalhes:".concat(e.getMessage)))
      }
    }
    processStatus
  }
}
