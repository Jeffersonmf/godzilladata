package config

object Environment {

  val conf = com.typesafe.config.ConfigFactory.load()

  def getJsonSourceFolder(): String = {
    conf.getString("Configuration.Local.Json_SourceFolder")
  }

  def getParquetDestinationFolder(): String = {
    conf.getString("Configuration.Local.Parquet_DestinationFolder")
  }

  def getPurgeDestinationFolder(): String = {
    conf.getString("Configuration.Local.Purge_DestinationFolder")
  }

  def isRunningAWSMode(): Boolean = {
    conf.getBoolean("Configuration.Setup.Running_AWS_Mode")
  }

  def aws_access_key(): String = {
    conf.getString("Configuration.AWS.fs.s3a.access.key")
  }

  def aws_secret_key(): String = {
    conf.getString("Configuration.AWS.fs.s3a.secret.key")
  }

  def get_AWS_JsonSourceFolder(): String = {
    conf.getString("Configuration.AWS.S3_SourceFolder")
  }

  def get_AWS_ParquetDestinationFolder(): String = {
    conf.getString("Configuration.AWS.S3_Parquet_DestinationFolder")
  }
}
