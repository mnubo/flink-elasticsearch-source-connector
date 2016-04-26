package com.mnubo.flink.streaming.connectors

import com.mnubo.flink.streaming.connectors.elasticsearch1x.{Elasticseach1xInputFormat, PojoRowDeserializer, TupleRowDeserializer}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.operators.DataSource
import org.apache.flink.api.java.typeutils.{PojoTypeInfo, TupleTypeInfoBase}
import org.apache.flink.api.scala._

import scala.reflect.ClassTag

object ElasticsearchDataset {
  /**
    * Creates a dataset from the given query. Simplified queries are not supported. Queries with aggregations are not supported. Queries must include a 'fields' list.
    * Fields will be deserialized in the corresponding fields of the tuple or case class, in the same order. They will be mapped in the corresponding pojoFields in the order
    * they appear in pojoFields.
    *
    * The query can only target a single index.
    *
    * @param env The Flink Scala execution environment.
    * @param index The Elasticsearch index name
    * @param query The Elasticsearch query. It must include a 'fields' list (https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-fields.html).
    * @param nodes An initial Elasticsearch cluster nodes that will be used to discover the full cluster. Must include at least one node of the cluster.
    * @param port The Elasticsearch HTTP port
    * @param pojoFields When reading in a POJO Java class, the list of the POJO class in which deserializing the fields from Elasticsearch.
    * @tparam T The type of record to emmit query results to.
    * @return A Flink Scala DataSet.
    */
  def fromElasticsearch1xQuery[T : ClassTag: TypeInformation](env: ExecutionEnvironment,
                                                              index: String,
                                                              query: String,
                                                              nodes: Set[String] = Set("localhost"),
                                                              port: Int = 9200,
                                                              pojoFields: Array[String] = null): DataSet[T] = {
        val typeInfo = implicitly[TypeInformation[T]]

        val inputFormat =
          typeInfo match {
            case info: TupleTypeInfoBase[T] =>
              new Elasticseach1xInputFormat[T](
                nodes,
                port,
                index,
                query,
                new TupleRowDeserializer[T](typeInfo.asInstanceOf[TupleTypeInfoBase[T]])
              )
            case info: PojoTypeInfo[T] =>
              require(pojoFields != null, "POJO fields must be specified (not null) if output type is a POJO.")

              new Elasticseach1xInputFormat[T](
                nodes,
                port,
                index,
                query,
                new PojoRowDeserializer[T](typeInfo.asInstanceOf[PojoTypeInfo[T]], pojoFields)
              )
            case _ =>
              throw new IllegalArgumentException(s"The type $typeInfo has to be a tuple or pojo type.")
          }

        new DataSet[T](new DataSource[T](env.getJavaEnv, inputFormat, typeInfo, getCallLocationName()))
  }


}
