package course3.project.RW

import java.io.BufferedInputStream
import course3.project.GTFS.{Calender, Routes, Trips}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import scala.io.Source
//
//Created by Dhrumil on 2020-02-26
//

class Reader{
  val conf = new Configuration()

  conf.addResource(new Path("/home/bd-user/opt/hadoop-2.7.3/etc/cloudera/core-site.xml"))
  conf.addResource(new Path("/home/bd-user/opt/hadoop-2.7.3/etc/cloudera/hdfs-site.xml"))
  val hadoop:FileSystem = FileSystem.get(conf)

  val filePathTrips = new BufferedInputStream( hadoop.open( new Path( "/user/fall2019/dhrumil/stm/trips.txt") ) )
  val filePathRoutes = new BufferedInputStream( hadoop.open( new Path( "/user/fall2019/dhrumil/stm/routes.txt") ) )
  val filePathCalender = new BufferedInputStream( hadoop.open( new Path( "/user/fall2019/dhrumil/stm/calendar.txt") ) )
//

  def getRouteList:List[Routes] = {
    val source = Source.fromInputStream(filePathRoutes)
    val input = source.getLines().drop(1)
      .map(line => line.split(","))
      .map(a => Routes(a(0).toInt, a(1), a(2), a(3), a(4), a(5), a(6))).toList
    source.close()
    input
  }

  def getCalenderList: List[Calender] = {
    val source = Source.fromInputStream(filePathCalender)
    val input = source.getLines().drop(1)
      .map(line => line.split(","))
      .map(a => Calender(a(0), a(1), a(2), a(3), a(4), a(5), a(6), a(7), a(8), a(9))).toList
    source.close()
    input
  }

  def getTripList: List[Trips] = {
    val source = Source.fromInputStream(filePathTrips)
    val input = source.getLines().drop(1)
      .map(line => line.split(","))
      .map(a => Trips(a(0).toInt, a(1), a(2), a(3), a(4), a(5), a(6))).toList
    source.close()
    input
  }
}

