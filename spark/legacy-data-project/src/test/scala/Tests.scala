
import config.Environment
import org.scalatest._

class Tests extends FlatSpec with Matchers {

  /**
   * Some tests in Scala using native BDD and infix concepts.
   *
   * @author Jefferson Marchetti Ferreira
   *
   */
  it should "return non-null String of destination folder value" in {

    Environment.getParquetDestinationFolder(Environment.isRunningLocalMode()) shouldNot be(null)
  }

  it should "return non-null String of source Json folder value" in {

    Environment.getParquetDestinationFolder(Environment.isRunningLocalMode()) shouldNot be(null)
  }

  it should "return non-null String of purge folder value" in {

    Environment.getHistoryDestinationFolder(Environment.isRunningLocalMode()) shouldNot be(null)
  }

  //  it should "enrich the data in the source folder" in {
  //
  //    val result = EnrichmentEngine.enrichmentSourceData()
  //
  //    if(result)
  //      result should be(true)
  //    else
  //      result should be(false)
  //  }
  //
  //  it should "load already enriched data filtered by anonymous user session id" in {
  //    val loadSessionByUser = EnrichmentEngine.loadSessionByUser("5C5AD855-52EA-4D41-B83D-D27844A9F8DE")
  //  }
  //
  //  it should "load already enriched data filtered by Browser type of user" in {
  //    val mappingByBrowser = EnrichmentEngine.mappingByBrowser("5C5AD855-52EA-4D41-B83D-D27844A9F8DE")
  //  }
  //
  //  it should "load already enriched data filtered by OS type of user" in {
  //    val mappingByOS = EnrichmentEngine.mappingByOS("5C5AD855-52EA-4D41-B83D-D27844A9F8DE")
  //  }
  //
  //  it should "load already enriched data filtered by Device type of user" in {
  //    val mappingByDevices = EnrichmentEngine.mappingByDevices("5C5AD855-52EA-4D41-B83D-D27844A9F8DE")
  //  }
  //
  //  it should "catching an Load Data Exception" in {
  //    a[Exception] should be thrownBy {
  //      throw new LoadDataException("It's just only a test..")
  //    }
  //  }
  //
  //  it should "catching an Purge Data Exception" in {
  //    a[Exception] should be thrownBy {
  //      throw new PurgeDataException("It's just only a test..")
  //    }
  //  }
}