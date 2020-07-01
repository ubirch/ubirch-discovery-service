package com.ubirch.discovery.util

import java.io.File
import java.net.{ Socket, SocketException, URL }
import java.nio.file.{ Files, Path, Paths }
import java.util.zip.ZipFile

import com.typesafe.scalalogging.LazyLogging

import scala.collection.JavaConverters._
import scala.sys.process._

object RemoteJanusGraph extends LazyLogging {

  def startJanusGraphServer(): Unit = {

    prepareJanusgraphFiles()

    if (!isJanusAlreadyStarted) {
      val janusgraphShPath = "src/test/resources/embedded-jg/janusgraph-0.5.1/bin/gremlin-server.sh"
      val janusgrapPropsPath = "../custom-gremlin-conf.yaml"
      s"bash $janusgraphShPath $janusgrapPropsPath".run

      Thread.sleep(30000)
      logger.info("ah que coucou")
      val janusgraphGremlinPath = "src/test/resources/embedded-jg/janusgraph-0.5.1/bin/gremlin.sh"
      val indexPath = "src/test/resources/embedded-jg/custom-index.txt"
      s"echo $indexPath".!

      s"bash $janusgraphGremlinPath" #< s"cat $indexPath" !

      logger.info("JanusGraph server started and index created")
    } else {
      logger.info("janus is already running (or port 8183 is unavailable)")
    }

  }

  def isJanusAlreadyStarted: Boolean = {
    isPortInUse("127.0.0.1", 8183)
  }

  private def isPortInUse(host: String, port: Int) = {
    var result = false
    try {
      new Socket(host, port).close()
      result = true
    } catch {
      case _: SocketException =>
      // Could not connect.
    }
    result
  }

  private def prepareJanusgraphFiles(): Unit = {
    val url = "https://github.com/JanusGraph/janusgraph/releases/download/v0.5.1/janusgraph-0.5.1.zip"
    val janusGraphZipFileName = "src/test/resources/embedded-jg/jgServer.zip"
    val janusGraphZipFilePath = Paths.get(janusGraphZipFileName)
    val outputPath = Paths.get("src/test/resources/embedded-jg/janusgraph-0.5.1")
    if (!Files.exists(outputPath)) {
      logger.info("janusgraph folder doesn't exist")
      if (!Files.exists(janusGraphZipFilePath)) {
        logger.info("janusgraph.zip doesn't exist, downloading...")
        downloadFile(url, janusGraphZipFileName)
        logger.info("janusgraph.zip download complete")
      }
      logger.info("unzipping")
      unzip(janusGraphZipFilePath, outputPath.getParent)
    }
  }

  private def downloadFile(url: String, filename: String): String = {
    logger.info("downloading file")
    new URL(url) #> new File(filename) !!
  }

  private def unzip(zipPath: Path, outputPath: Path): Unit = {
    val zipFile = new ZipFile(zipPath.toFile)
    for (entry <- zipFile.entries.asScala) {
      val path = outputPath.resolve(entry.getName)
      if (entry.isDirectory) {
        Files.createDirectories(path)
      } else {
        Files.createDirectories(path.getParent)
        Files.copy(zipFile.getInputStream(entry), path)
      }
    }
  }

}
