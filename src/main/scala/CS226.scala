
import java.io.File
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.locationtech.jts.geom.Coordinate




object CS226{

	def getFile(file:File): Array[File] ={
		val files = file.listFiles().filter(! _.isDirectory)
			.filter(t => t.toString.endsWith(".tif"))  //此处读取.txt and .md文件
		files ++ file.listFiles().filter(_.isDirectory).flatMap(getFile)
	}

	def main(args: Array[String]): Unit = {

		// parse the arguments
		var data_path : String = "/opt/hadoop/home/hadoop2/data/roads_sample"
		var dem_count = 1
		if (args.length.equals(1) || args.length.equals(2)){
			data_path = args(0)
			if (args.length.equals(2)){
				dem_count = args(1).toInt
			}
		}

		// inital spark session
		val spark = SparkSession
			.builder()
			.getOrCreate()

		import spark.implicits._

		// load original data and sep them by tab then filter out the null values
		val raw_data = spark.read.option("delimiter", "\t").csv(data_path).filter(!_.isNullAt(1))



		// Get the linestring and polygon data and combine them
		val linestring = raw_data.rdd.map(x => (x.getString(0),x.getString(1)))
			.filter(x => x._2.contains("LINESTRING"))
			.map(x => (x._1, x._2.replace("LINESTRING (","")
				.replace(")","")))
		val lineploy = raw_data.rdd.map(x => (x.getString(0),x.getString(1)))
			.filter(x => x._2.contains("POLYGON"))
			.map(x => (x._1, x._2.replace("POLYGON ((","")
				.replace("))","")))
		val lines = linestring.union(lineploy)

		// convert the coordinates of each points to float type
		val lines_double = lines.map(x => (x._1, x._2.split(", ")
			.toList.map(_.split(" ")
			.toList.map(_.toFloat))))
		// convert the points coordinates info to Coordinate structure  line_info =  <road_id, List[point_coordinates]>
		var line_info = lines_double.map(x => (x._1, x._2.map(y => new Coordinate(y(0).toDouble, y(1).toDouble))))

		// flatmap the road_info to point_info. point_info = <road_id, (point_coordinate, index)>.
		var point_info = line_info.flatMap(x =>x._2.zipWithIndex.map(y => (x._1, (y._1, y._2))))
			.repartition(100).persist(StorageLevel.MEMORY_ONLY)
		line_info.unpersist()
		lines.unpersist()
		lineploy.unpersist()
		linestring.unpersist()


		//Get tif files array
		val path = new File("/opt/hadoop/home/hadoop2/data/DEM")
		val tif_files = getFile(path)


		var count = 0

		for(pp<-tif_files.take(dem_count).toIterable) { // for each file

			println("=========================" + count.toString + "==========================")
			count = count + 1

			// Load tif file
			val geoTiff = GeoTiffReader.readSingleband(pp.toString)
			val raster_info = geoTiff.rasterExtent // Raster Information
			val tile = geoTiff.tile //tile info

			// Get tif files boundry
			val x_min = raster_info.extent.xmin
			val x_max = raster_info.extent.xmax
			val y_min = raster_info.extent.ymin
			val y_max = raster_info.extent.ymax

			// Select filtered data
			val point_filtered = point_info // select the points in this boundary
				.filter(x => x._2._1.x >= x_min && x._2._1.x <= x_max && x._2._1.y >= y_min && x._2._1.y <= y_max)
				.repartition(100)
			point_info.unpersist()

			// Get the altitude data from raster info
			val point_filtered3d = point_filtered.map(point => {
				val grid = raster_info.mapToGrid(point._2._1.x, point._2._1.y) // get the index of the point
				var z = tile.getDouble(grid._1, grid._2) // use index to find the data we need
				if (z <= -1000){ // if there is no data in that point
					z = 0.0}
				point._2._1.setZ(z)
				point
			})
			point_filtered.unpersist()

			// Collect the point_info
			val point_list = point_filtered3d.map(x => (x._1, List(x._2))).reduceByKey(_ ++ _)
				.map(x => (x._1, x._2.sortBy(_._2))) // (id, List(coordinate, index)). sort by index
				.map(x => x._1 + "\t"  + x._2.map(point => point._1.getX.toString + "," + point._1.y.toString + "," + point._1.z.toString).mkString("\t"))// convert it to string type

			point_filtered3d.unpersist()

			point_list.repartition(100).saveAsTextFile("/opt/hadoop/home/hadoop2/data/output/dem" + count.toString()) // save the file
			point_list.unpersist()

		}

	}
}
