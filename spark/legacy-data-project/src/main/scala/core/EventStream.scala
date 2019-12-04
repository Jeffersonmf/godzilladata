package core

import net.caoticode.dirwatcher.FSListener

class EnrichmentFileListener extends FSListener {
  import java.nio.file.Path

  override def onCreate(ref: Path): Unit = EnrichmentEngine.chargeSourceData()
  override def onDelete(ref: Path): Unit = println(s"Purge routine are running.")
  override def onModify(ref: Path): Unit = EnrichmentEngine.chargeSourceData()
}
