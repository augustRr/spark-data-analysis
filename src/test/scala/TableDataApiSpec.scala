import org.scalatest.BeforeAndAfterAll
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import scalaj.http.HttpResponse

class TableDataApiSpec extends AnyFunSpec with Matchers with BeforeAndAfterAll {

  // Define the base URL for the API
  val baseUrl = "http://localhost:8080/api/table-data"

  // Override beforeAll to set up any necessary data or configuration
  override def beforeAll(): Unit = {
    // Start your Spark application separately here
    // This could involve running a JAR on a Spark cluster or locally
    // You need to ensure that your TableDataApi is running and accessible at the baseUrl
  }

  // Override afterAll to clean up resources after testing
  override def afterAll(): Unit = {
    // Stop your Spark application separately here
    // This could involve stopping the Spark cluster or the locally running Spark application
  }

  describe("TableDataApi") {

    it("should return table data without aggregation by default") {
      val response: HttpResponse[String] = scalaj.http.Http(baseUrl).asString
      // Assertions based on the response
      response.code shouldEqual 200
      // Add more assertions based on the expected response
    }

    it("should return table data with aggregation when requested") {
      val urlWithParams = s"$baseUrl?groupBy=gender&aggregateAverage=true&aggregateMax=true"
      val response: HttpResponse[String] = scalaj.http.Http(urlWithParams).asString
      // Assertions based on the response
      response.code shouldEqual 200
      // Add more assertions based on the expected response
    }

    // Add more test cases as needed to cover different scenarios
  }
}
