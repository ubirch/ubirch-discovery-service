package com.ubirch.discovery.core

//class Benchmarks extends FeatureSpec with Matchers {
//
//  implicit var gc: GremlinConnector = _
//
//  private val dateTimeFormat = ISODateTimeFormat.dateTime()
//
//  val fileName = getClass.getResource("/benchmarks.txt")
//
//  val Number: Key[String] = Key[String]("number")
//  val Name: Key[String] = Key[String]("name")
//  val Created: Key[String] = Key[String]("created")
//  val IdAssigned: Key[String] = Key[String]("IdAssigned")
//
//  def log: Logger = LoggerFactory.getLogger(this.getClass)
//
//  def deleteDatabase(): Unit = {
//    gc = GremlinConnector.get
//    gc.g.V().drop().iterate()
//    gc.closeConnection()
//  }
//
//  def truc(): Unit = {
//
//    // prepare
//    val id1 = new Random().nextInt(1000000000)
//    val id2 = new Random().nextInt(1000000000)
//
//    val now1 = DateTime.now(DateTimeZone.UTC)
//    val p1: List[KeyValue[String]] = List(
//      new KeyValue[String](Number, "5"),
//      new KeyValue[String](Name, "aName1"),
//      new KeyValue[String](Created, dateTimeFormat.print(now1))
//    )
//    val now2 = DateTime.now(DateTimeZone.UTC)
//    val p2: List[KeyValue[String]] = List(
//      new KeyValue[String](Number, "6"),
//      new KeyValue[String](Name, "aName2"),
//      new KeyValue[String](Created, dateTimeFormat.print(now2))
//    )
//    val pE: List[KeyValue[String]] = List(
//      new KeyValue[String](Name, "edge")
//    )
//
//    //commit
//    val t0 = System.nanoTime()
//    AddVertices().addTwoVertices(id1.toString, p1)(id2.toString, p2)(pE)
//
//    // analyse
//    val v1Reconstructed = new VertexStructDb(id1.toString, gc.g)
//    val v2Reconstructed = new VertexStructDb(id2.toString, gc.g)
//
//    val arrayKeys = Array(IdAssigned, Name, Created, Number)
//
//    val response1 = v1Reconstructed.getPropertiesMap
//    val idGottenBack1 = extractValue[String](response1, IdAssigned.name)
//    val propertiesReceived1 = recompose(response1, arrayKeys)
//
//    val response2 = v2Reconstructed.getPropertiesMap
//    val idGottenBack2 = extractValue[String](response2, IdAssigned.name)
//    val propertiesReceived2 = recompose(response2, arrayKeys)
//
//    propertiesReceived1.sortBy(x => x.key.name) shouldBe p1.sortBy(x => x.key.name)
//    propertiesReceived2.sortBy(x => x.key.name) shouldBe p2.sortBy(x => x.key.name)
//    id1.toString shouldBe idGottenBack1
//    id2.toString shouldBe idGottenBack2
//    val t1 = System.nanoTime()
//
//    val pw = new BufferedWriter(new FileWriter(fileName.getPath, true))
//    pw.write((t1 / 1000000 - t0 / 1000000).toString + System.getProperty("line.separator"))
//    pw.close()
//    log.info("time elapsed= " + (t1 / 1000000 - t0 / 1000000).toString + "ms")
//
//  }
//
//  scenario("add create two new vertex on an empty db, link them, verify that the result is correct") {
//    gc = GremlinConnector.get
//    for (i <- 0 to 200) {
//      log.info(s"step $i")
//      truc()
//    }
//    val br = new BufferedReader(new FileReader(fileName.getPath))
//    var line: String = ""
//    var sum = 0
//    var nbLines = 0
//    var stillGoing = true
//    while (stillGoing) {
//      line = br.readLine()
//      if (line != null) {
//        sum += line.toInt
//        nbLines += 1
//      } else stillGoing = false
//    }
//    log.info("average = " + (sum / nbLines).toString + "ms")
//  }
//
//}
