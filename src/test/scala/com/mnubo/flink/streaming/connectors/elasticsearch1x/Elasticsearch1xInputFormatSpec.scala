package com.mnubo.flink.streaming.connectors.elasticsearch1x

import com.mnubo.flink.streaming.connectors.{DataRow, ElasticsearchDataset}
import org.apache.flink.api.scala._
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}

class Elasticsearch1xInputFormatSpec extends WordSpec with Matchers with BeforeAndAfterAll {
  private val Index = "e1if_test_index"
  private val EmptyIndex = "e1if_empty_index"
  private val es = new ElasticsearchTestServer()

  "The Elasticseach 1.X input format" should {
    "fetch a DataSet from Elasticsearch to a case class" in {
      val sut = ElasticsearchDataset.fromElasticsearch1xQuery[CaseESDocument](
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
      val sut = ElasticsearchDataset.fromElasticsearch1xQuery[DataRow](
        ExecutionEnvironment.getExecutionEnvironment,
        Index,
        """{"fields": ["some_string","some_boolean","some_long","some_date","sub_doc.sub_doc_id"]}""",
        Set(es.host),
        es.httpPort
      )

      sut.filter(_[String]("some_string") != "def").collect() should contain only(
        DataRow(
          "abc", true, 12345678901L, "2016-04-25T21:54:23.321Z", "sd1"
        ),
        DataRow(
          Array(null, false, 98765432109L, null, "sd2"),
          classOf[String], classOf[Boolean], classOf[Long], classOf[String], classOf[String]
        )
      )
    }
    "fetch a DataSet from Elasticsearch to a data row and perform fancy logic" in {
      val sut = ElasticsearchDataset.fromElasticsearch1xQuery[DataRow](
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
      val sut = ElasticsearchDataset.fromElasticsearch1xQuery[(String, Boolean, Long, String, String)](
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
      val sut = ElasticsearchDataset.fromElasticsearch1xQuery[PojoESDocument](
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
      val sut = ElasticsearchDataset.fromElasticsearch1xQuery[(String, Boolean, Long, String, String)](
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

    es.client
      .admin
      .indices
      .prepareCreate(Index)
      .addMapping(DocType, mapping)
      .get

    es.client
      .admin
      .indices
      .prepareCreate(EmptyIndex)
      .addMapping(DocType, mapping)
      .get

    es.client
      .prepareIndex(Index, DocType)
      .setSource("""{"some_string": "abc", "some_boolean": "true", "some_long": 12345678901, "some_date": "2016-04-25T21:54:23.321Z", "sub_doc": {"sub_doc_id": "sd1"}}""")
      .get

    es.client
      .prepareIndex(Index, DocType)
      .setSource("""{"some_string": "def", "some_boolean": "true", "some_long": 10, "some_date": "2015-04-25T21:54:23.321Z", "sub_doc": {"sub_doc_id": "sd1"}}""")
      .get

    es.client
      .prepareIndex(Index, DocType)
      .setSource("""{"some_string": null, "some_boolean": "false", "some_long": 98765432109, "sub_doc": {"sub_doc_id": "sd2"}}""")
      .get

    es.client
      .admin
      .indices
      .prepareFlush(Index)
      .setForce(true)
      .get
  }

  override def afterAll() =
    es.close()

}
