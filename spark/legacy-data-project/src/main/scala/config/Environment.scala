package config

object Environment {

  val conf = com.typesafe.config.ConfigFactory.load()

  def getJsonSourceFolder(): String = {
    conf.getString("Configuration.Enrichment.Json_SourceFolder")
  }

  def getParquetDestinationFolder(): String = {
    conf.getString("Configuration.Enrichment.Parquet_DestinationFolder")
  }

  def getPurgeDestinationFolder(): String = {
    conf.getString("Configuration.Enrichment.Purge_DestinationFolder")
  }
}
