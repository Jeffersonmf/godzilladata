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

  def runningMode(): Boolean = {
    conf.getBoolean("Configuration.Setup.Running_Mode")
  }

  def aws_access_key(): String = {
    conf.getString("Configuration.AWS.fs.s3a.access.key")
  }

  def aws_secret_key(): String = {
    conf.getString("Configuration.AWS.fs.s3a.secret.key")
  }
}
