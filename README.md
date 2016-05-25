# Apache Flink source connector for Elasticsearch

Allow to pipe the result of an Elasticsearch query into a Flink data set. Supports scala &amp; java tuples, case classes, POJO, and a variable length result set called DataRow.

## Usage:

### buil.sbt

    libraryDependencies += "com.mnubo" %% "flink-elasticsearch-source-connector" % "1.0.0-flink1"

### then:

    import com.mnubo.flink.streaming.connectors.DataRow
    import com.mnubo.flink.streaming.connectors.elasticsearch.ElasticsearchDataset
    import org.apache.flink.api.scala._

    val esIndexName = "my_es_index"

    val esNodeHostNames = Set("es_node_1", "es_node_2", "es_node_3")

    val esHttpPort = 9200

    val esQuery = """{"fields": ["some_string","some_boolean","some_long","some_date","sub_doc.sub_doc_id"]}"""

    val dataSet = ElasticsearchDataset.fromElasticsearchQuery[DataRow](
      ExecutionEnvironment.getExecutionEnvironment,
      esIndexName,
      esQuery,
      esNodeHostNamess,
      esHttpPort
    )

    dataSet
      .groupBy("sub_doc.sub_doc_id")
      .sum(2)
      .print

The Elasticsearch query must contain a `fields` field.

Aggregations are not supported.

Tested with Elasticsearch 1.5.2, 1.7.5, and 2.3.3.

