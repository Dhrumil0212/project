package course3.project.Mapping
import course3.project.GTFS.Calender


//
//  Created by Dhrumil on 2020-02-19
//

class CalendarLookup (calendars: List[Calender]){

  private val lookupTable: Map[String, Calender] =
    calendars.map(calendar => calendar.service_id -> calendar).toMap

  def lookup(serviceId: String): Calender = lookupTable.getOrElse(serviceId, null)
}
