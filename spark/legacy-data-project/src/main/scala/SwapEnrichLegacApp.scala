import akka.actor.ActorSystem
import colossus.IOSystem
import colossus.core.InitContext
import colossus.protocols.http.server.{HttpServer, Initializer}
import config.Environment
import routers.HttpRouterHandler

object SwapEnrichLegacApp extends App {

  implicit val actorSystem = ActorSystem()
  implicit val ioSystem = IOSystem()

  //Initializing Enrichment Data with Spark.
  //EnrichmentEngine.chargeSourceData()

  //Initializing File Watcher to the Enrichment future data.
  val directoryPath = Environment.getJsonSourceFolder()

//  val watcher = DirWatcher()
//  watcher.watchFor(directoryPath, new EnrichmentFileListener())
//  watcher.start()

  HttpServer.start("Scala-Colossus", 9000){ context => new EnrichmentInitializer(context) }
}

class EnrichmentInitializer(context: InitContext) extends Initializer(context) {
  override def onConnect = context => new HttpRouterHandler(context)
}