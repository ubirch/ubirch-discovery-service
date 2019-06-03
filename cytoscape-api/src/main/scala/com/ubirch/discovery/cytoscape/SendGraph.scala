package com.ubirch.discovery.cytoscape

import com.ubirch.discovery.core.connector.GremlinConnector

class SendGraph {

  implicit val gc: GremlinConnector = new GremlinConnector

}
