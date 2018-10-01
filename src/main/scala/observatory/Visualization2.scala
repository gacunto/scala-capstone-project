package observatory

import com.sksamuel.scrimage.{Image}
import scala.math._

/**
  * 5th milestone: value-added information visualization
  */
object Visualization2 {

  private val imageWidth: Int = 256
  private val imageHeight: Int = 256
  private val alpha: Int = 127

  /**
    * @param point (x, y) coordinates of a point in the grid cell
    * @param d00   Top-left value
    * @param d01   Bottom-left value
    * @param d10   Top-right value
    * @param d11   Bottom-right value
    * @return A guess of the value at (x, y) based on the four known values, using bilinear interpolation
    *         See https://en.wikipedia.org/wiki/Bilinear_interpolation#Unit_Square
    */
  def bilinearInterpolation(
                             point: CellPoint,
                             d00: Temperature,
                             d01: Temperature,
                             d10: Temperature,
                             d11: Temperature
                           ): Temperature = {
    val x = point.x
    val y = point.y

    d00 * (1 - x) * (1 - y) +
      d10 * x * (1 - y) +
      d01 * (1 - x) * y +
      d11 * x * y
  }

  /**
    * @param grid   Grid to visualize
    * @param colors Color scale to use
    * @param tile   Tile coordinates to visualize
    * @return The image of the tile at (x, y, zoom) showing the grid using the given color scale
    */
  def visualizeGrid(
                     grid: GridLocation => Temperature,
                     colors: Iterable[(Temperature, Color)],
                     tile: Tile
                   ): Image = {
    val x = tile.x
    val y = tile.y
    val zoom = tile.zoom

    val locations = for {
      i <- (0 until imageWidth)
      j <- 0 until imageHeight
      xTile = i.toDouble / imageWidth + x
      yTile = j.toDouble / imageHeight + y
    } yield j * imageWidth + i -> Interaction.toLocation(xTile, yTile, zoom)

    val pixels = locations.par.map {
      case (i, loc) =>

        val latRange = List(floor(loc.lat).toInt, ceil(loc.lat).toInt)
        val lonRange = List(floor(loc.lon).toInt, ceil(loc.lon).toInt)

        val dx = loc.lon - lonRange.head
        val dy = latRange(1) - loc.lat

        val d = {
          for {
            xPos <- 0 to 1
            yPos <- 0 to 1
          } yield (xPos, yPos) -> grid(GridLocation(latRange(1 - yPos), lonRange(xPos)))
        }.toMap


        val temperature = bilinearInterpolation(CellPoint(dx, dy), d((0, 0)), d((0, 1)), d((1, 0)), d((1, 1)))
        val color = Visualization.interpolateColor(colors, temperature)
        (i, color.toPixel(alpha))
    }

    Image(imageWidth, imageHeight, pixels.toArray.sortBy(_._1).map(_._2))
  }

}
