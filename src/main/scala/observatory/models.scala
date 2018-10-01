package observatory

import com.sksamuel.scrimage.Pixel
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType}
import org.apache.spark.sql.{Encoder, Encoders}

import scala.math._
import scala.reflect.ClassTag

/**
  * Introduced in Week 1. Represents a location on the globe.
  * @param lat Degrees of latitude, -90 ≤ lat ≤ 90
  * @param lon Degrees of longitude, -180 ≤ lon ≤ 180
  */
case class Location(lat: Double, lon: Double) {
  private val R = 6371e3

  private def distanceTo(loc: Location): Double = {
    val dLat = (loc.lat - lat).toRadians
    val dLon = (loc.lon - lon).toRadians

    val a = sin(dLat / 2.0) * sin(dLat / 2.0) + cos(lat.toRadians) * cos(loc.lat.toRadians) * sin(dLon / 2.0) * sin(dLon / 2.0)
    val c = 2.0 * atan2(sqrt(a), sqrt(1.0 - a))

    R * c
  }

  def idw(loc: Location): Double = {
    1.0 / pow(distanceTo(loc), Location.P)
  }

  def isAt(x: Int, y: Int): Boolean = {
    lat.toInt == x && lon.toInt == y
  }

  def toGridLocation: GridLocation = GridLocation(lat.toInt, lon.toInt)
}

object Location {

  val P: Double = 2.0

  def fromPixelIndex(index: Int): Location = {
    def x: Int = index % 360
    def y: Int = index / 360

    Location(90 - y, x - 180)
  }

  def fromPixelIndexZoomXY(index: Int, zoom: Int, x: Int, y: Int): Location = {
    def x0: Int = (index % 256) / 256 + x
    def y0: Int = (index / 256) / 256 + y

    val p: Double = (1 << zoom).toDouble

    def lat: Double = atan(sinh(Pi * (1.0 - 2.0 * y0.toDouble / p))) * 180.0 / Pi
    def lon: Double = (x0.toDouble * 360.0) / p - 180.0

    Location(lat, lon)
  }
}

/**
  * Introduced in Week 3. Represents a tiled web map tile.
  * See https://en.wikipedia.org/wiki/Tiled_web_map
  * Based on http://wiki.openstreetmap.org/wiki/Slippy_map_tilenames
  * @param x X coordinate of the tile
  * @param y Y coordinate of the tile
  * @param zoom Zoom level, 0 ≤ zoom ≤ 19
  */
case class Tile(x: Int, y: Int, zoom: Int)

/**
  * Introduced in Week 4. Represents a point on a grid composed of
  * circles of latitudes and lines of longitude.
  * @param lat Circle of latitude in degrees, -89 ≤ lat ≤ 90
  * @param lon Line of longitude in degrees, -180 ≤ lon ≤ 179
  */
case class GridLocation(lat: Int, lon: Int) {
  def toLocation: Location = Location(lat, lon)
}

/**
  * Introduced in Week 5. Represents a point inside of a grid cell.
  * @param x X coordinate inside the cell, 0 ≤ x ≤ 1
  * @param y Y coordinate inside the cell, 0 ≤ y ≤ 1
  */
case class CellPoint(x: Double, y: Double)

/**
  * Introduced in Week 2. Represents an RGB color.
  * @param red Level of red, 0 ≤ red ≤ 255
  * @param green Level of green, 0 ≤ green ≤ 255
  * @param blue Level of blue, 0 ≤ blue ≤ 255
  */
case class Color(red: Int, green: Int, blue: Int) {
  def toPixel(alpha: Int): Pixel = {
    Pixel(red, green, blue, alpha)
  }
}

/**
  *
  * @param stn
  * @param wban
  * @param lat
  * @param lon
  */
case class Station(stn: Option[Int], wban: Option[Int], lat: Option[Double], lon: Option[Double])

/**
  *
  */
object Station {
  val stationsStructType =
    StructType(Seq(
      StructField("stn", IntegerType, nullable = true),
      StructField("wban", IntegerType, nullable = true),
      StructField("lat", DoubleType, nullable = true),
      StructField("lon", DoubleType, nullable = true)
    ))
}

case class Record(stn: Option[Int], wban: Option[Int], month: Int, day: Int, temp: Double)

object Record {
  val temperatureStructType = StructType(Seq(
    StructField("stn", IntegerType, nullable = true),
    StructField("wban", IntegerType, nullable = true),
    StructField("month", IntegerType, nullable = false),
    StructField("day", IntegerType, nullable = false),
    StructField("temp", DoubleType, nullable = false)
  ))
}

object ObservatoryImplicits {

  implicit class DoubleToRGB(value: Double) {
    def toRGB: Int = max(0, min(255, math.round(value).toInt))
  }

  implicit class F2C(f: Double) {
    def toCelsius: Double = ((f - 32.0) * 5.0) / 9.0
  }

  implicit def kryoEncoder[A](implicit ct: ClassTag[A]): Encoder[A] =
    org.apache.spark.sql.Encoders.kryo[A](ct)

  implicit def tuple3[A1, A2, A3](implicit e1: Encoder[A1], e2: Encoder[A2], e3: Encoder[A3]): Encoder[(A1, A2, A3)] =
    Encoders.tuple[A1, A2, A3](e1, e2, e3)
}
