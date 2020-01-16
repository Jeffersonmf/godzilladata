import akka.actor.ActorSystem
import colossus.IOSystem
import colossus.core.InitContext
import colossus.protocols.http.server.{HttpServer, Initializer}
import routers.HttpRouterHandler

object SwapEnrichLegacyApp extends App {

  implicit val actorSystem = ActorSystem()
  implicit val ioSystem = IOSystem()

  HttpServer.start("Scala-Colossus", 9000){ context => new EnrichmentInitializer(context) }
}

class EnrichmentInitializer(context: InitContext) extends Initializer(context) {
  override def onConnect = context => new HttpRouterHandler(context)
}