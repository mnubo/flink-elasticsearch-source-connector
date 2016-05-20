# Apache Flink source connector for Elasticsearch

Allow to pipe the result of an Elasticsearch query into a Flink data stream. Supports scala & java tuples, case classes, POJO, and a variable length result set called DataRow.

Usage:

    import com.mnubo.flink.streaming.connectors.{DataRow, ElasticsearchDataset}
    import org.apache.flink.api.scala._

    val esIndexName = "my_es_index"

    val esNodeHostNamess = Set("es_node_1", "es_node_2", "es_node_3")

    val esHttpPort = 9300

    val esQuery = """{"fields": ["some_string","some_boolean","some_long","some_date","sub_doc.sub_doc_id"]}"""

    val dataSet = ElasticsearchDataset.fromElasticsearch1xQuery[DataRow](
      ExecutionEnvironment.getExecutionEnvironment,
      esIndexName,
      esQuery,
      esNodeHostNamess,
      esHttpPort
    )

    println(dataSet.collect())


