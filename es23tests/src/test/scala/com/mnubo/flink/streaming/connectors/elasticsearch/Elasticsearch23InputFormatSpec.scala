package com.mnubo.flink.streaming.connectors.elasticsearch

import java.net.InetAddress

import com.mnubo.flink.streaming.connectors._
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.cluster.health.ClusterHealthStatus
import org.elasticsearch.common.transport.InetSocketTransportAddress

class Elasticsearch23InputFormatSpec extends ElasticsearchInputFormatSpec {
  protected override def elasticSearchVersion = "2.3.3"

  private def createClientInternal(host: String = es.host, port: Int = es.esTransportPort) =
    TransportClient
      .builder()
      .build()
      .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port))

  protected override def isClusterGreen(host: String, port: Int) =
    using(createClientInternal(host, port))(c => c.admin.cluster.prepareHealth().get.getStatus == ClusterHealthStatus.GREEN)

  protected override def createClient: ESClient = new ESClient {
    private val internalClient = createClientInternal()

    override def createIndex(indexName: String, docType: String, mapping: String) =
      internalClient
        .admin
        .indices
        .prepareCreate(indexName)
        .addMapping(docType, mapping)
        .get

    override def flush(indexName: String) =
      internalClient
        .admin
        .indices
        .prepareFlush(indexName)
        .setForce(true)
        .get


    override def index(indexName: String, docType: String, doc: String) =
      internalClient
        .prepareIndex(indexName, docType)
        .setSource(doc)
        .get

    override def close() =
      internalClient.close()
  }
}
