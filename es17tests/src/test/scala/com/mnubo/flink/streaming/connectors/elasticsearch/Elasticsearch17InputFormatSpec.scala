package com.mnubo.flink.streaming.connectors.elasticsearch

import com.mnubo.flink.streaming.connectors._
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.transport.InetSocketTransportAddress

class Elasticsearch17InputFormatSpec extends ElasticsearchInputFormatSpec {
  protected override def elasticSearchVersion = "1.7.5"

  private def createClientInternal(host: String = es.host, port: Int = es.esTransportPort) =
    new TransportClient()
      .addTransportAddress(new InetSocketTransportAddress(host, port))

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
