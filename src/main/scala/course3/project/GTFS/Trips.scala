package course3.project.GTFS

//
//  Created by Dhrumil on 2020-02-06
//

case class Trips(route_id:Int,service_id:String,trip_id:String,trip_headsign:String,direction_id:String,
                 shape_id:String,wheelchair_accessible:String,note_fr:String="",note_en:String ="")

