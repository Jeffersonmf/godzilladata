package history

import java.sql.Date

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.col
import org.joda.time.DateTime

case class ProcessedStackHistory(appName: String, fileSourceName: String, dateTime: DateTime)

object HistoryOfExecutions {

  //def

//  /**
//    * Salva na tabela de historico de execucoes as informacoes da atividade que foi executada
//    *
//    * @param nomeDaTabela    nome da tabela que foi trabalhada, este campo e utilizado como referencia para encontrar a ultimava vez que algo foi feito
//    * @param totalAdicionado quantidade de records que foram adicionados a tabela
//    * @param totalDuplicados quantidade de dados duplicados que foram removidos
//    * @param sc
//    * @param spark
//    */
//  def salvaHistorico(nomeDaTabela: String, totalAdicionado: Long, totalDuplicados: Long, duracao: Long, sc: SparkContext, spark: SparkSession, dia: Date): Unit = {
//
//    import spark.implicits._
//
//    // adiciona informacoes no historico de execucoes
//    val diaAnterior: String = DateTime.now().minusDays(1).toString("yyyy-MM-dd")
//
//    // capturas as informações do cluster em um arquivo na maquina EMR
//    var cluster: String = ""
//    if (!runLocal) {
//      val rdd: RDD[(String, String)] = sc.wholeTextFiles("file:///mnt/var/lib/info/job-flow.json")
//      rdd.collect().foreach(cluster += _)
//    }
//
//    var df: DataFrame = Seq.apply(
//      (
//        dia,
//        nomeDaTabela,
//        java.sql.Date.valueOf(diaAnterior),
//        cluster,
//        sc.applicationId,
//        totalAdicionado,
//        totalDuplicados,
//        duracao
//      )
//    )
//      .toDF("dia", "tabela", "ultima_atualizacao", "cluster_infos", "application_id", "numero_de_dados", "numero_de_duplicados", "tempo_de_execucao")
//
//    df.coalesce(1).write.mode(SaveMode.Append).parquet(Paths.getOutputHistoricoExecucoes)
//  }
//
//  def buscaHistorico(nomeDaTabela: String, sc: SparkContext, spark: SparkSession): Option[DateTime] = {
//
//    var dia: Option[DateTime] = None
//
//    // verifica a existencia da tabela de historico de execucoes
//    if (Paths.pathExist(Paths.getOutputHistoricoExecucoes, sc)) {
//      val historico_de_execucoes = spark.read.parquet(Paths.getOutputHistoricoExecucoes)
//        .toDF("dia", "tabela", "ultima_atualizacao", "cluster_infos", "application_id", "numero_de_dados", "numero_de_duplicados", "tempo_de_execucao")
//
//      // pega a ultima data em que foi transformada
//      val refs = historico_de_execucoes.select("dia").filter(s"tabela = '${nomeDaTabela}'")
//        .orderBy(col("dia").desc)
//
//      val referencia: Option[String] =
//        if (refs.count() > 0) Some(refs.first().getDate(0).toString) else None
//
//      // executa o processo de ETL somente para os paths que ainda nao foram transformados caso exista algum que ja foi feito
//      if (referencia.isDefined) {
//
//        val date: Array[String] = referencia.get.split("-")
//
//        dia = Some((new DateTime)
//          .withYear(Integer.parseInt(date.apply(0)))
//          .withMonthOfYear(Integer.parseInt(date.apply(1)))
//          .withDayOfMonth(Integer.parseInt(date.apply(2).split("T")(0))))
//      }
//    }
//
//    return dia
//  }
}
