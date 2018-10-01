package observatory

import scala.collection.parallel.ParMap

/**
  * 4th milestone: value-added information
  */
object Manipulation {

  private lazy val locations = for (lat <- -89 to 90; lon <- -180 to 179) yield Location(lat, lon)

  /**
    * @param temperatures Known temperatures
    * @return A function that, given a latitude in [-89, 90] and a longitude in [-180, 179],
    *         returns the predicted temperature at this location
    */
  def makeGrid(temperatures: Iterable[(Location, Temperature)]): GridLocation => Temperature = {
    val grid: ParMap[Location, Temperature] =
      locations.par
        .map(loc => loc -> Visualization.predictTemperature(temperatures, loc))
        .toMap
    gl: GridLocation => grid(gl.toLocation)
  }

  /**
    * @param temperaturess Sequence of known temperatures over the years (each element of the collection
    *                      is a collection of pairs of location and temperature)
    * @return A function that, given a latitude and a longitude, returns the average temperature at this location
    */
  def average(temperaturess: Iterable[Iterable[(Location, Temperature)]]): GridLocation => Temperature = {
    val grids: Iterable[GridLocation => Temperature] = temperaturess.map(makeGrid)
    val averages: ParMap[Location, Temperature] =
      locations.par
        .map(loc => loc -> grids.aggregate(0.0)(_ + _ (loc.toGridLocation), _ + _) / grids.size.toDouble)
        .toMap
    gl: GridLocation => averages(gl.toLocation)
  }

  /**
    * @param temperatures Known temperatures
    * @param normals      A grid containing the “normal” temperatures
    * @return A grid containing the deviations compared to the normal temperatures
    */
  def deviation(temperatures: Iterable[(Location, Temperature)], normals: GridLocation => Temperature): GridLocation => Temperature = {
    val grid = makeGrid(temperatures)
    gl: GridLocation => {
      // Standard deviation
      //math.sqrt(math.pow(grid(x, y) - normals(x, y), 2) / temperatures.size)
      // Just deviation
      grid(gl) - normals(gl)
    }
  }

}

