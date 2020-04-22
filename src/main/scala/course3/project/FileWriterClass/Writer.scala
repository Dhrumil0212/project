package course3.project.FileWriterClass

import java.io.OutputStreamWriter
import au.com.bytecode.opencsv.CSVWriter
import course3.project.GTFS.EnrichedTrip
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

class Writer(enrichedList: List[EnrichedTrip]) {
  val conf = new Configuration()

  conf.addResource(new Path("/home/bd-user/opt/hadoop-2.7.3/etc/cloudera/core-site.xml"))
  conf.addResource(new Path("/home/bd-user/opt/hadoop-2.7.3/etc/cloudera/hdfs-site.xml"))

  val hadoop:FileSystem = FileSystem.get(conf)
  val out = new OutputStreamWriter( hadoop.create( new Path( "/user/fall2019/dhrumil/course3/finaloutput.csv"), true) )

  val csvWriter = new CSVWriter(out)
  val csvHeader: Array[String] = Array("Route Id", "Service Id", "Trip Id", "Trip Head Sign", "Direction Id",
    "Shape Id", "Wheelchair accessible", "Note_FR", "Note En", "Agency Id",
    "Route Short Name", "Route Long Name", "Route Type", "Route Url", "Route Colour",
    "Monday")
  var Records = new ListBuffer[Array[String]]()
  Records += csvHeader

  for{element <- enrichedList} yield Records += Array(
    element.tripRoute.routes.route_id.toString,
    element.calender.service_id.toString,
    element.tripRoute.trips.trip_id.toString, element.tripRoute.trips.trip_headsign.toString,
    element.tripRoute.trips.direction_id.toString, element.tripRoute.trips.shape_id.toString,
    element.tripRoute.trips.wheelchair_accessible.toString, element.tripRoute.trips.note_fr.toString,
    element.tripRoute.trips.note_en.toString, element.tripRoute.routes.agency_id.toString,
    element.tripRoute.routes.route_short_name.toString, element.tripRoute.routes.route_long_name.toString,
    element.tripRoute.routes.route_type.toString, element.tripRoute.routes.route_url.toString,
    element.tripRoute.routes.route_color.toString, element.calender.monday.toString)
  csvWriter.writeAll(Records.asJava)
  out.close()
}