package com.mnubo.flink.streaming.connectors
package elasticsearch

import org.scalatest.concurrent.Eventually
import org.scalatest.time.SpanSugar

import scala.sys.process._
import scala.util.Try

class ElasticsearchTestServer(version: String, isClusterGreen: (String, Int) => Boolean) extends AutoCloseable with Eventually with SpanSugar {
  private val hasRecoveredIndicesStateRegex = """recovered \[\d+\] indices into cluster_state""".r
  val host = {
    val hostVar = System.getenv("DOCKER_HOST")
    if (hostVar != null)
      """\d+\.[0-9\.]+""".r
        .findFirstIn(hostVar)
        .getOrElse("127.0.0.1")
    else
      "127.0.0.1"
  }
  val containerId = s"docker run -d -P elasticsearch:$version --network.publish_host $host".!!.trim
  val httpPort = esPort(9200)
  val esTransportPort = esPort(9300)

  eventually(timeout(20.seconds), interval(500.millis)) {
    require(hasRecoveredIndicesState && isClusterGreen(host, esTransportPort), "ES Still not started...")
  }

  override def close() = {
    Try(s"docker stop $containerId".!)
    Try(s"docker rm $containerId".!)
  }

  private def hasRecoveredIndicesState = {
    val logs = s"docker logs $containerId".!!
    hasRecoveredIndicesStateRegex.findFirstIn(logs).isDefined
  }

  private def esPort(exposedPort: Int) = Seq(
    "docker",
    "inspect",
    s"""--format='{{(index (index .NetworkSettings.Ports "$exposedPort/tcp") 0).HostPort}}'""",
    containerId
  ).!!.trim.toInt
}
