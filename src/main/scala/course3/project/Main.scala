package course3.project
import course3.project.FileWriterClass.Writer
import course3.project.GTFS._
import course3.project.Mapping._
import course3.project.RW.Reader
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

object Main extends App{

  val conf = new Configuration()

  conf.addResource(new Path("/home/bd-user/opt/hadoop-2.7.3/etc/cloudera/core-site.xml"))
  conf.addResource(new Path("/home/bd-user/opt/hadoop-2.7.3/etc/cloudera/hdfs-site.xml"))

  val hadoop:FileSystem = FileSystem.get(conf)
  val fs: FileSystem = FileSystem.get(conf)

  println(fs.getUri)
  println(fs.getWorkingDirectory)
  println("=======================================")
  fs
    .listStatus(new Path("/user/fall2019/dhrumil"))
    .map(_.getPath)
    .foreach(println)
  println("========================================")
  println(fs.getUri)
  //========================================================================================
  val readData : Reader = new Reader

  val tripList: List[Trips] = readData.getTripList
  val routeList: List[Routes] = readData.getRouteList
  val calendarList: List[Calender] = readData.getCalenderList

  val routeLookup = new RouteLookup(routeList)
  val calenderLookUp = new CalendarLookup(calendarList)
  val enrichedTripRoute: List[TripRoute] = tripList.map(trip => TripRoute(trip,
    routeLookup.lookup(trip.route_id)))

  val enrichedTrip: List[EnrichedTrip] = enrichedTripRoute.map(tripRoute => EnrichedTrip(tripRoute,
    calenderLookUp.lookup(tripRoute.trips.service_id)))

  val finalListOfTrips: List[EnrichedTrip] = enrichedTrip.filter(a => a.calender.monday.equals("1") && a.tripRoute.routes.route_type.equals("1"))
  val writer = new Writer(finalListOfTrips)
  writer.csvWriter
}

