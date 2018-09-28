package observatory

import java.nio.file.Paths
import java.time.LocalDate

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}


/**
  * 1st milestone: data extraction
  */
object Extraction {

  /**
    * Configure Spark session and logger
    */
  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("observatory")
      .config("spark.master", "local[*]")
      .config("spark.network.timeout", "600")
      .getOrCreate()

  import observatory.ObservatoryImplicits._
  import spark.implicits._

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  /**
    * @param year             Year number
    * @param stationsFile     Path of the stations resource file to use (e.g. "/stations.csv")
    * @param temperaturesFile Path of the temperatures resource file to use (e.g. "/1975.csv")
    * @return A sequence containing triplets (date, location, temperature)
    */
  def locateTemperatures(year: Year, stationsFile: String, temperaturesFile: String): Iterable[(LocalDate, Location, Temperature)] = {
    val stationsDs = readStations(stationsFile)
    val temperaturesDs = readTemperatures(temperaturesFile)

    stationsDs.join(temperaturesDs,
      stationsDs("stn").eqNullSafe(temperaturesDs("stn")) &&
        stationsDs("wban").eqNullSafe(temperaturesDs("wban")))
      .map(row => (
        LocalDate.of(year, row.getAs[Int]("month"), row.getAs[Int]("day")),
        Location(row.getAs[Double]("lat"), row.getAs[Double]("lon")),
        row.getAs[Double]("temp").toCelsius
      ))
      .persist()
      .collect()
  }

  /**
    *
    * @param stationsFile
    * @return
    */
  private def readStations(stationsFile: String): Dataset[Station] = {
    readCsvFile(stationsFile, Station.stationsStructType)
      .as[Station]
      .filter((station: Station) => station.lat.isDefined && station.lon.isDefined)
  }

  /**
    *
    * @param temperaturesFile
    * @return
    */
  private def readTemperatures(temperaturesFile: String): Dataset[Record] = {
    readCsvFile(temperaturesFile, Record.temperatureStructType)
      .as[Record]
      .filter((record: Record) => record.temp != 9999.9)
  }

  /**
    *
    * @param resource
    * @return
    */
  private def resourcePath(resource: String): String = {
    Paths.get(getClass.getResource(resource).toURI).toString
  }

  /**
    *
    * @param csvFilePath
    * @param csvSchema
    * @return
    */
  private def readCsvFile(csvFilePath: String, csvSchema: StructType): DataFrame = {
    spark.read
      .option("sep", ",")
      .option("mode", "FAILFAST")
      .option("header", false)
      .option("inferSchema", false)
      .schema(csvSchema)
      .csv(resourcePath(csvFilePath))
  }

  /**
    * @param records A sequence containing triplets (date, location, temperature)
    * @return A sequence containing, for each location, the average temperature over the year.
    */
  def locationYearlyAverageRecords(records: Iterable[(LocalDate, Location, Temperature)]): Iterable[(Location, Temperature)] = {
    spark.sparkContext
      .parallelize(records.toSeq)
      .map(r => (r._1.getYear, r._2, r._3))
      .toDF("year", "loc", "temp")
      .groupBy($"year", $"loc")
      .agg($"year", $"loc", avg($"temp").as("temp"))
      .select($"loc".as[Location], $"temp".as[Double])
      .persist()
      .collect()
  }

}
