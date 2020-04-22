package course3.project.Mapping

import course3.project.GTFS.Routes

//
//  Created by Dhrumil on 2020-02-19
//

class RouteLookup(routes: List[Routes]) {

  private val lookupTable: Map[Int, Routes] =
    routes.map(route => route.route_id -> route).toMap

  def lookup(routeId: Int): Routes = lookupTable.getOrElse(routeId, null)

}
