package observatory

import com.sksamuel.scrimage.Image

import scala.math._
import Visualization._

/**
  * 3rd milestone: interactive visualization
  */
object Interaction {

  def imageWidth: Int = 256

  def imageHeight: Int = 256

  def imageAlpha: Int = 127

  /**
    * @param tile Tile coordinates
    * @return The latitude and longitude of the top-left corner of the tile, as per http://wiki.openstreetmap.org/wiki/Slippy_map_tilenames
    */
  def tileLocation(tile: Tile): Location = {
    val p = 1 << tile.zoom
    Location(
      toDegrees(atan(sinh(Pi * (1.0 - 2.0 * tile.y.toDouble / p)))),
      tile.x.toDouble / p * 360.0 - 180.0
    )
  }

  /**
    * @param temperatures Known temperatures
    * @param colors       Color scale
    * @param tile         Tile coordinates
    * @return A 256×256 image showing the contents of the given tile
    */
  def tile(temperatures: Iterable[(Location, Temperature)], colors: Iterable[(Temperature, Color)], tile: Tile): Image = {
    val width = 256
    val height = 256
    val alpha = 127

    val pixels = (0 until width * height)
      .par.map(pos => {
      val xPos = (pos % width).toDouble / width + tile.x
      val yPos = (pos / height).toDouble / height + tile.y

      val temperature = predictTemperature(
        temperatures,
        toLocation(xPos, yPos, tile.zoom)
      )

      val color: Color = interpolateColor(colors, temperature)

      pos -> color.toPixel(alpha)
    }
    )
      .seq
      .sortBy(_._1)
      .map(_._2).toArray

    Image(width, height, pixels)
  }

  def toLocation(x: Double, y: Double, zoom: Int): Location = {
    Location(
      lat = toDegrees(atan(sinh(Pi * (1.0 - 2.0 * y / (1 << zoom))))),
      lon = x / (1 << zoom) * 360.0 - 180.0)
  }

  /**
    * Generates all the tiles for zoom levels 0 to 3 (included), for all the given years.
    *
    * @param yearlyData    Sequence of (year, data), where `data` is some data associated with
    *                      `year`. The type of `data` can be anything.
    * @param generateImage Function that generates an image given a year, a zoom level, the x and
    *                      y coordinates of the tile and the data to build the image from
    */
  def generateTiles[Data](
                           yearlyData: Iterable[(Year, Data)],
                           generateImage: (Year, Tile, Data) => Unit
                         ): Unit = {

    val tiles = for {
      (year, data) <- yearlyData
      zoom <- 0 to 3
      x <- 0 until 1 << zoom
      y <- 0 until 1 << zoom
    } yield (year, Tile(x, y, zoom), data)

    tiles.par.foreach(t => {
      generateImage(t._1, t._2, t._3)
    })
  }

}
