package com.example.etl.ingestion

import com.example.etl.config.{ApiSourceConfig, ApiPaginationConfig, ApiPaginationStrategy}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions.col // For selecting nested data using dataPath
import scala.util.{Try, Success, Failure}
import org.slf4j.LoggerFactory
import scalaj.http.{Http, HttpRequest, HttpResponse}
import spray.json._ // For parsing JSON response to find next page URL or data path
// Assuming simple DefaultJsonProtocol for parsing parts of the response, not for the main data.
// Main data parsing into DataFrame is done by Spark.


object ApiIngestor {
  private val logger = LoggerFactory.getLogger(this.getClass)
  private val MAX_PAGES_DEFAULT = 100 // Default safety for pagination

  def ingestApiData(
      spark: SparkSession,
      apiConfig: ApiSourceConfig,
      jobId: String // For logging context
  ): Try[DataFrame] = {

    var allDfs = List[DataFrame]()
    var nextPageUrl: Option[String] = Some(apiConfig.url)
    var pagesFetched = 0
    val maxPages = apiConfig.pagination.flatMap(_.maxPages).getOrElse(MAX_PAGES_DEFAULT)

    Try {
      while (nextPageUrl.isDefined && pagesFetched < maxPages) {
        val currentUrl = nextPageUrl.get
        pagesFetched += 1
        logger.info(s"Job [$jobId]: Fetching API data from URL: $currentUrl (Page: $pagesFetched)")

        var request: HttpRequest = Http(currentUrl)
          .method(apiConfig.method.toUpperCase)

        apiConfig.headers.getOrElse(List()).foreach { header =>
          request = request.header(header.name, header.value)
        }

        // Add request body for POST/PUT if present (simplified for now)
        if ((apiConfig.method.equalsIgnoreCase("POST") || apiConfig.method.equalsIgnoreCase("PUT")) && apiConfig.requestBody.isDefined) {
          request = request.postData(apiConfig.requestBody.get)
          // Potentially set content-type header if not already set by user
          if (!apiConfig.headers.getOrElse(List()).exists(_.name.equalsIgnoreCase("Content-Type"))) {
            logger.warn(s"Job [$jobId]: Request body provided for POST/PUT but no Content-Type header set. Assuming application/json.")
            request = request.header("Content-Type", "application/json")
          }
        }

        // Execute request
        val response: HttpResponse[String] = request.asString

        if (response.isSuccess) {
          logger.info(s"Job [$jobId]: Successfully fetched data from $currentUrl (Status: ${response.code})")
          val responseBody = response.body
          if (responseBody.trim.isEmpty) {
            logger.warn(s"Job [$jobId]: API response body from $currentUrl is empty. Stopping pagination if this was the current page.")
            nextPageUrl = None // Stop if empty response
          } else {
            // Parse response based on apiPayloadType
            apiConfig.apiPayloadType.toUpperCase match {
              case "JSON" =>
                // Use Spark to parse the main data payload
                // Create a DataFrame from the JSON string(s).
                // Spark expects JSON Lines format (one JSON object per line) or a single JSON array/object.
                // If the API returns a single JSON object containing an array, dataPath is crucial.
                // If the API returns a JSON array directly as root, dataPath might be "" or "$".

                // Create a Dataset[String] from the response body to feed into spark.read.json
                // This handles cases where the response is a large JSON array or many JSON objects.
                val responseDs = spark.createDataset(Seq(responseBody))(spark.implicits.newStringEncoder)
                var pageDf = spark.read.json(responseDs)

                // Apply dataPath to extract the actual data array if specified
                // This is a simplified dataPath handling. Complex JSONPath needs more robust parsing.
                apiConfig.dataPath.filter(_.trim.nonEmpty) match {
                  case Some(path) =>
                    try {
                      // Attempt to select the column/path. This works if path is a top-level field.
                      // For nested paths like "data.items", this should work.
                      // For JSONPath like "$.results[*]", this simple col select won't work.
                      // A more robust solution would parse the JSON with Spray/Circe to extract the array,
                      // then convert that array of JSON objects to a JSON string for Spark.
                      // Or, explode() if the path leads to an array within the DataFrame structure.
                      if (path.contains(".") || path.contains("[")) { // Basic check for nested/array path
                         logger.info(s"Job [$jobId]: Applying nested dataPath: '$path'")
                         // This assumes the path resolves to a struct or array that can be further processed.
                         // If it's an array to be exploded: pageDf = pageDf.select(explode(col(path)).as("data_root"))
                         // If it's a struct: pageDf = pageDf.select(s"$path.*")
                         // For now, assume dataPath points to a field that contains the records, possibly an array of structs.
                         pageDf = pageDf.select(s"$path.*") // This might fail if path is not a struct.
                                                 // A safer way if path leads to an array of structs:
                                                 // pageDf = pageDf.select(explode(col(path))).select("col.*")
                                                 // For now, this is simplified.
                         logger.info(s"Job [$jobId]: Extracted data using dataPath '$path'. Schema: ${pageDf.schema.simpleString}")
                    } catch {
                        case e: Exception =>
                            logger.error(s"Job [$jobId]: Failed to apply dataPath '$path' to JSON response from $currentUrl. Error: ${e.getMessage}. Using raw JSON structure.")
                            // Proceed with pageDf as is, or could fail the page load.
                    }
                  case None =>
                    logger.info(s"Job [$jobId]: No dataPath specified or path is empty. Using entire JSON response structure.")
                }
                allDfs = pageDf :: allDfs

                // Pagination: NEXT_PAGE_URL strategy
                nextPageUrl = None // Reset for this page
                apiConfig.pagination.filter(_.strategy.equalsIgnoreCase(ApiPaginationStrategy.NextPageUrl.toString)) match {
                  case Some(paginationConfig) =>
                    paginationConfig.nextPageUrlPath.filter(_.trim.nonEmpty) match {
                      case Some(nextUrlPath) =>
                        Try {
                          val jsonAst = responseBody.parseJson // spray-json
                          // Simple path extractor for next page URL (e.g., "links.next")
                          var currentJsValue: JsValue = jsonAst
                          nextUrlPath.split('.').foreach { key =>
                            currentJsValue = currentJsValue.asJsObject.fields(key)
                          }
                          currentJsValue match {
                            case JsString(url) if url.trim.nonEmpty => nextPageUrl = Some(url)
                            case _ => logger.info(s"Job [$jobId]: Next page URL path '$nextUrlPath' not found or not a string in response from $currentUrl.")
                          }
                        } recover {
                          case ex: Exception => logger.warn(s"Job [$jobId]: Could not extract next page URL using path '$nextUrlPath' from $currentUrl. Error: ${ex.getMessage}")
                        }
                      case None => logger.info(s"Job [$jobId]: Pagination strategy is NEXT_PAGE_URL, but no nextPageUrlPath is defined in config.")
                    }
                  case None => // No pagination or different strategy
                     logger.info(s"Job [$jobId]: No 'NEXT_PAGE_URL' pagination strategy configured or applicable. Fetched single page/batch.")
                     // nextPageUrl is already None if not NEXT_PAGE_URL strategy
                }
              case other =>
                logger.error(s"Job [$jobId]: Unsupported API payload type: $other from $currentUrl")
                throw new IllegalArgumentException(s"Unsupported API payload type: $other")
            } // end match apiPayloadType
          } // end else responseBody not empty
        } else { // HTTP request failed
          val errorMsg = s"Job [$jobId]: Failed to fetch data from API $currentUrl. Status: ${response.code}, Body: ${response.body.take(500)}"
          logger.error(errorMsg)
          throw new RuntimeException(errorMsg)
        }
      } // end while loop

      if (allDfs.isEmpty) {
        logger.warn(s"Job [$jobId]: No data fetched from API source ${apiConfig.url} after $pagesFetched pages.")
        // Return an empty DataFrame with a placeholder schema if no schema was ever inferred.
        // Or, ideally, if a schema was provided via config.schemaMappings, use that.
        // For now, Spark will create an empty DF if the list is empty.
        // To avoid issues downstream, ensure it has *some* schema.
        // If spark.read.json was never successful, allDfs would be empty and reduce will fail.
        // If it was successful once but yielded empty DF, then that schema is used.
        spark.emptyDataFrame // Or Try a specific schema if available from config (TODO)
      } else {
        // Union all DataFrames from paginated calls
        // To handle varying schemas across pages (though ideally they are consistent):
        // 1. Find a common schema (merge schemas - complex)
        // 2. Or, use the schema of the first page and assume others conform (simpler, but risky)
        // Spark's unionByName will fill with nulls if columns don't match.
        // Let's assume for now pages have compatible schemas.
        val finalDf = allDfs.reduce((df1, df2) => df1.unionByName(df2, allowMissingColumns = true)) // Spark 3.1+ for allowMissingColumns
        logger.info(s"Job [$jobId]: Finished fetching all pages. Total pages: ${pagesFetched}. Final DataFrame schema: ${finalDf.schema.simpleString}")
        finalDf
      }
    } // End of Try
  } // End of ingestApiData
}
