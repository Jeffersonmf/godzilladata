package config

object Environment {

  val conf = com.typesafe.config.ConfigFactory.load()

  def getSourceFolder(isLocal: Boolean): String = {
    val sourceKey = if(isLocal) "Local" else "AWS"
    val configValue = s"Configuration.$sourceKey.SourceFolder"
    conf.getString(configValue)
  }

  def getParquetDestinationFolder(isLocal: Boolean): String = {
    val sourceKey = if(isLocal) "Local" else "AWS"
    val configValue = s"Configuration.$sourceKey.Parquet_DestinationFolder"
    conf.getString(configValue)
  }

  def getHistoryDestinationFolder(isLocal: Boolean): String = {
    val sourceKey = if(isLocal) "Local" else "AWS"
    conf.getString(s"Configuration.$sourceKey.History_DestinationFolder")
  }

  def isRunningLocalMode(): Boolean = {
    conf.getBoolean("Configuration.Setup.Running_Local_Mode")
  }

  def aws_access_key(): String = {
    conf.getString("Configuration.AWS.fs.s3a.access.key")
  }

  def aws_secret_key(): String = {
    conf.getString("Configuration.AWS.fs.s3a.secret.key")
  }
}