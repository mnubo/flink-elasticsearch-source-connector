package com.mnubo.flink.streaming.connectors
package elasticsearch

import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.scala._
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}

abstract class ElasticsearchInputFormatSpec extends WordSpec with Matchers with BeforeAndAfterAll {
  protected def elasticSearchVersion: String
  protected def isClusterGreen(host: String, port: Int): Boolean
  protected def createClient: ESClient

  private val Index = "e1if_test_index"
  private val EmptyIndex = "e1if_empty_index"
  protected val es = new ElasticsearchTestServer(elasticSearchVersion, isClusterGreen)

  s"The Elasticseach $elasticSearchVersion input format" should {
    "fetch a DataSet from Elasticsearch to a case class" in {
      val sut = ElasticsearchDataset.fromElasticsearchQuery[CaseESDocument](
        ExecutionEnvironment.getExecutionEnvironment,
        Index,
        """{"fields": ["some_string","some_boolean","some_long","some_date","sub_doc.sub_doc_id"]}""",
        Set(es.host),
        es.httpPort
      )

      sut.filter(_.str != "def").collect() should contain only(
        CaseESDocument("abc", boo = true, 12345678901L, "2016-04-25T21:54:23.321Z", "sd1"),
        CaseESDocument(null, boo = false, 98765432109L, null, "sd2")
        )
    }
    "fetch a DataSet from Elasticsearch to a data row" in {
      val sut = ElasticsearchDataset.fromElasticsearchQuery[DataRow](
        ExecutionEnvironment.getExecutionEnvironment,
        Index,
        """{"fields": ["some_string","some_boolean","some_long","some_date","sub_doc.sub_doc_id"]}""",
        Set(es.host),
        es.httpPort
      )

      def build(values:Any*) = {
        val expectedTypes = Array[java.lang.Class[_]](classOf[String], classOf[Boolean], classOf[Long], classOf[String], classOf[String]).map(TypeExtractor.getForClass(_))
        val expectedNames = Array("some_string","some_boolean","some_long","some_date","sub_doc.sub_doc_id")
        DataRow(values.indices.map { i => Value(values(i), expectedNames(i), expectedTypes(i)) }: _*)
      }

      sut.filter(_[String]("some_string") != "def").collect() should contain only(
        build("abc", true, 12345678901L, "2016-04-25T21:54:23.321Z", "sd1"),
        build(null, false, 98765432109L, null, "sd2")
      )
    }
    "fetch a DataSet from Elasticsearch to a data row and perform fancy logic" in {
      val sut = ElasticsearchDataset.fromElasticsearchQuery[DataRow](
        ExecutionEnvironment.getExecutionEnvironment,
        Index,
        """{"fields": ["some_string","some_boolean","some_long","some_date","sub_doc.sub_doc_id"]}""",
        Set(es.host),
        es.httpPort
      )

      sut
        .groupBy("sub_doc.sub_doc_id")
        .sum(2)
        .map(row => (row[Long]("some_long"), row[String]("sub_doc.sub_doc_id")))
        .collect() should contain only(
          (98765432109L, "sd2"),
          (12345678911L, "sd1")
        )
    }
    "fetch a DataSet from Elasticsearch to a Scala tuple" in {
      val sut = ElasticsearchDataset.fromElasticsearchQuery[(String, Boolean, Long, String, String)](
        ExecutionEnvironment.getExecutionEnvironment,
        Index,
        """{"fields": ["some_string","some_boolean","some_long","some_date","sub_doc.sub_doc_id"]}""",
        Set(es.host),
        es.httpPort
      )

      sut.filter(_._1 != "def").collect() should contain only(
        ("abc", true, 12345678901L, "2016-04-25T21:54:23.321Z", "sd1"),
        (null, false, 98765432109L, null, "sd2")
      )
    }
    "fetch a DataSet from Elasticsearch to a Pojo" in {
      val sut = ElasticsearchDataset.fromElasticsearchQuery[PojoESDocument](
        ExecutionEnvironment.getExecutionEnvironment,
        Index,
        """{"fields": ["some_string","some_boolean","some_long","some_date","sub_doc.sub_doc_id"]}""",
        Set(es.host),
        es.httpPort,
        pojoFields = Array("str", "boo", "lon", "date", "sub")
      )

      sut.filter(_.str != "def").collect() should contain only(
        new PojoESDocument("abc", true, 12345678901L, "2016-04-25T21:54:23.321Z", "sd1"),
        new PojoESDocument(null, false, 98765432109L, null, "sd2")
      )
    }
    "fetch an empty DataSet from Elasticsearch" in {
      val sut = ElasticsearchDataset.fromElasticsearchQuery[(String, Boolean, Long, String, String)](
        ExecutionEnvironment.getExecutionEnvironment,
        EmptyIndex,
        """{"fields": ["some_string","some_boolean","some_long","some_date","sub_doc.sub_doc_id"]}""",
        Set(es.host),
        es.httpPort
      )

      sut.collect() shouldBe empty
    }
  }

  override def beforeAll() = {
    val DocType = "test_doc"
    val mapping =
      s"""
         |{
         |  "$DocType": {
         |    "properties": {
         |      "some_string": {"type": "string", "index": "not_analyzed"},
         |      "some_boolean": {"type": "boolean"},
         |      "some_long": {"type": "long"},
         |      "some_date": {"type": "date", "format": "date_time"},
         |      "sub_doc":{
         |        "type": "nested",
         |        "properties": {
         |          "sub_doc_id": {"type": "string", "index": "not_analyzed"}
         |        }
         |      }
         |    }
         |  }
         |}
         |""".stripMargin

    using(createClient) { esClient =>
      esClient.createIndex(Index, DocType, mapping)

      esClient.createIndex(EmptyIndex, DocType, mapping)

      esClient.index(
        Index,
        DocType,
        """{"some_string": "abc", "some_boolean": "true", "some_long": 12345678901, "some_date": "2016-04-25T21:54:23.321Z", "sub_doc": {"sub_doc_id": "sd1"}}"""
      )

      esClient.index(
        Index,
        DocType,
        """{"some_string": "def", "some_boolean": "true", "some_long": 10, "some_date": "2015-04-25T21:54:23.321Z", "sub_doc": {"sub_doc_id": "sd1"}}"""
      )

      esClient.index(
        Index,
        DocType,
        """{"some_string": null, "some_boolean": "false", "some_long": 98765432109, "sub_doc": {"sub_doc_id": "sd2"}}"""
      )

      esClient.flush(Index)
    }
  }

  override def afterAll() =
    es.close()

}

trait ESClient extends AutoCloseable {
  def flush(indexName: String)
  def index(indexName: String, docType: String, doc: String)
  def createIndex(indexName: String, docType: String, mapping: String)
}

class Elasticsearch15InputFormatSpec extends ElasticsearchInputFormatSpec {
  protected override def elasticSearchVersion = "1.5.2"

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
