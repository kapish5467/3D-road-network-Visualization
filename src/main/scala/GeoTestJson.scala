
import java.io.File
import java.lang.IndexOutOfBoundsException

import scala.collection.mutable.ListBuffer
import geotrellis.raster.merge.{RasterMergeMethods, TileFeatureMergeMethods, TileMergeMethods}
import geotrellis.raster.{RasterExtent, Tile}
import geotrellis.raster.io.geotiff.SinglebandGeoTiff
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.locationtech.jts.geom.Coordinate




object GeoTestJson{

	def getFile(file:File): Array[File] ={
		val files = file.listFiles().filter(! _.isDirectory)
			.filter(t => t.toString.endsWith(".tif"))  //此处读取.txt and .md文件
		files ++ file.listFiles().filter(_.isDirectory).flatMap(getFile)
	}

	def main(args: Array[String]): Unit = {

		val spark = SparkSession
			.builder()
			.getOrCreate()
		import spark.implicits._


		val data_path = ("/opt/hadoop/home/hadoop2/data/roads")
		val raw_data = spark.read.option("delimiter", "\t").csv(data_path).filter(!_.isNullAt(1))

		println("total number of data:" + raw_data.count())


		//				// Get the lines and transform the coordinates to Long
		val linestring = raw_data.rdd.map(x => (x.getString(0),x.getString(1))).filter(x => x._2.contains("LINESTRING")).map(x => (x._1, x._2.replace("LINESTRING (","").replace(")","")))
		val lineploy = raw_data.rdd.map(x => (x.getString(0),x.getString(1))).filter(x => x._2.contains("POLYGON")).map(x => (x._1, x._2.replace("POLYGON ((","").replace("))","")))
		val lines = linestring.union(lineploy)

		val lines_double = lines.map(x => (x._1, x._2.split(", ").toList.map(_.split(" ").toList.map(_.toFloat))))

		var line_info = lines_double.map(x => (x._1, x._2.map(y => new Coordinate(y(0).toDouble, y(1).toDouble))))

		var point_info = line_info.flatMap(x =>x._2.zipWithIndex.map(y => (x._1, (y._1, y._2))))




		//TIF
		val path = new File("/opt/hadoop/home/hadoop2/data/DEM")
		val tif_files = getFile(path)



		var count = 0


		val pp = tif_files.head
//		for(pp<-tif_files.toIterable) {
		println("=========================" + count.toString + "==========================")
		count = count + 1
		// tif info
		val geoTiff = GeoTiffReader.readSingleband(pp.toString)

		val raster_info = geoTiff.rasterExtent // Raster Information
		val tile = geoTiff.tile //tile info

		val x_min = raster_info.extent.xmin
		val x_max = raster_info.extent.xmax
		val y_min = raster_info.extent.ymin
		val y_max = raster_info.extent.ymax


		// Select filtered data
		val point_filtered = point_info.filter(x => x._2._1.x >= x_min && x._2._1.x <= x_max && x._2._1.y >= y_min && x._2._1.y <= y_max)
		point_info.unpersist()
		val point_filtered3d = point_filtered.map(point => {
			val grid = raster_info.mapToGrid(point._2._1.x, point._2._1.y)
			val z = tile.get(grid._1, grid._2)
			point._2._1.setZ(z)
			point
		})

		val point_list = point_filtered3d.map(x => (x._1, List(x._2))).reduceByKey(_ ++ _)
			.map(x => (x._1, x._2.sortBy(_._2))) // (id, List(coordinate))
			.map(x => x._1.toString + "," + x._2.map(point => List(point._1.x, point._1.y, point._1.z).mkString(" ")).mkString(","))

		point_list.saveAsTextFile("/opt/hadoop/home/hadoop2/data/output/dem" + count.toString())

//		}
	}
}
