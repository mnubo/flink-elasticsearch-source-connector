package com.mnubo.flink.streaming.connectors.elasticsearch1x

import java.lang.Float

import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode}
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.mnubo.flink.streaming.connectors.{MarshallerFieldDescriptor, RecordMarshaller}
import org.apache.flink.api.common.io.InputFormat
import org.apache.flink.api.common.io.statistics.BaseStatistics
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.io.InputSplitAssigner
import org.apache.flink.hadoop.shaded.org.apache.http.client.HttpClient
import org.apache.flink.hadoop.shaded.org.apache.http.client.methods._
import org.apache.flink.hadoop.shaded.org.apache.http.client.utils.URIBuilder
import org.apache.flink.hadoop.shaded.org.apache.http.entity.StringEntity
import org.apache.flink.hadoop.shaded.org.apache.http.impl.client.DefaultHttpClient
import org.slf4j.{Logger, LoggerFactory}

import scala.annotation.tailrec
import scala.collection.JavaConverters._

class Elasticseach1xInputFormat[T](nodes: Set[String],
                                   port: Int,
                                   index: String,
                                   query: String,
                                   deserializer: RecordMarshaller[T]) extends InputFormat[T, Elasticsearch1xInputSplit] {
  @transient
  private var log: Logger = null
  @transient
  private var httpClient: HttpClient = null
  @transient
  private var jsonParser: ObjectMapper = null
  @transient
  private var queriedFields: Seq[(String, Int)] = null

  private var currentSplit: Elasticsearch1xInputSplit = null
  private var currentScrollWindowId: String = null
  private var currentScrollWindowHits: ArrayNode = null
  private var nextRecordIndex = 0
  private var schema: Seq[MarshallerFieldDescriptor] = null

  override def configure(parameters: Configuration) = {
    // Configure all the transient fields.

    if (log == null)
      log = LoggerFactory.getLogger(classOf[Elasticseach1xInputFormat[T]])

    if (httpClient == null)
      httpClient = new DefaultHttpClient()

    if (jsonParser == null)
      jsonParser = new ObjectMapper()

    if (queriedFields == null) {
      val jsonQuery = jsonParser.readTree(query)

      require(jsonQuery.has("fields") && jsonQuery.get("fields").isArray, "The query must contains a 'fields' list: https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-fields.html.")

      queriedFields = jsonQuery
        .get("fields")
        .asInstanceOf[ArrayNode]
        .elements()
        .asScala
        .map(_.asText())
        .toSeq
        .zipWithIndex
    }
  }

  def fetchSchema() = {
    if (schema == null) {
      configure(null)
      val mappings =
        executeHttpQuery(getOnOneNode(s"/$index/_mappings"))
          .fields()
          .asScala

      val allFieldsIterator =
        for {
          indexEntry <- mappings
          typeEntry <- indexEntry.getValue.get("mappings").fields().asScala
          props = typeEntry.getValue.get("properties").fields().asScala.toList
          field <- fetchProperties(props)
        } yield field

      val allFields =
        allFieldsIterator
          .map { case mfd@MarshallerFieldDescriptor(name, _) => name -> mfd }
          .toMap

      schema =
        queriedFields
          .map { case (queryFieldName, _) =>
              val normalizedFieldName = queryFieldName

              require(allFields.contains(normalizedFieldName), s"Cannot found a field with a supported type named '$queryFieldName' in Elasticsearch index mapping '$index'")

              allFields(normalizedFieldName)
          }
          .toIndexedSeq
    }

    schema
  }

  @tailrec
  private def fetchProperties(properties: List[java.util.Map.Entry[String, JsonNode]],
                              prefix: String = "",
                              acc: Set[MarshallerFieldDescriptor] = Set.empty): Set[MarshallerFieldDescriptor] = properties match {
    case Nil =>
      acc
    case x :: xs =>
      val fieldName = prefix + x.getKey

      // nested object or array
      if (x.getValue.has("properties")) {
        val subProperties = x.getValue.get("properties").fields().asScala.toList
        fetchProperties(subProperties ::: xs, fieldName + ".", acc)
      }
      // simple type
      else {
        val fieldESType = x.getValue.get("type").asText()
        esSimpleMappings(fieldESType) match {
          case Some(clazz) =>
            // Supported type, add the field to the set
            fetchProperties(xs, prefix, acc + MarshallerFieldDescriptor(fieldName, clazz))
          case _ =>
            // Unsupported type, ignore the field
            fetchProperties(xs, prefix, acc)
        }
      }
  }

  override def createInputSplits(minNumSplits: Int): Array[Elasticsearch1xInputSplit] = {
    // Get the list of nodes and their published host name or IP
    val nodeIps = executeHttpQuery(getOnOneNode("/_nodes"))
      .get("nodes")
      .fields
      .asScala
      .map { nodeJsonField =>
        val nodeId = nodeJsonField.getKey
        val nodeDescriptorJsonObject = nodeJsonField.getValue
        val nodeIp = nodeDescriptorJsonObject.get("settings").get("network").get("publish_host").asText()

        nodeId -> nodeIp
      }
      .toMap

    // Get the list of shards, choosing a random replica for each of the shards
    val resVal =
      executeHttpQuery(getOnOneNode(s"/$index/_search_shards"))
        .get("shards")
        .elements
        .asScala
        .map { shardReplicasJsonArray =>
          val (_, shard, nodeId) = shardReplicasJsonArray
            .elements
            .asScala
            .map { replicaJsonObject =>
              import replicaJsonObject._
              (get("state").asText(), get("shard").asInt(), get("node").asText())
            }
            .filter(_._1 == "STARTED")
            .next() // Seems random enough for a start. TODO: try to maximize the number of nodes reached.

          Elasticsearch1xInputSplit(index, shard, nodeIps(nodeId), nodeId, port)
        }
       .toArray

    log.info("Generated splits: {}", resVal.mkString(", "))

    resVal
  }

  override def getInputSplitAssigner(inputSplits: Array[Elasticsearch1xInputSplit]): InputSplitAssigner = new InputSplitAssigner {
    var i = -1
    // Note: we are not on the same nodes as ES anyway, so don't try to match task manager nodes with ES nodes.
    override def getNextInputSplit(host: String, taskId: Int) =
      if (i >= inputSplits.length - 1)
        null
      else {
        i += 1
        inputSplits(i)
      }
  }

  /**
    * Not supported yet.
    *
    * @return null
    */
  override def getStatistics(cachedStatistics: BaseStatistics): BaseStatistics =
    null

  override def open(split: Elasticsearch1xInputSplit) = {
    log.info("Opening split: {}", split)

    currentSplit = split

    val uri = new URIBuilder()
      .setScheme("http")
      .setHost(currentSplit.nodeHost)
      .setPort(currentSplit.nodePort)
      .setPath(s"/$index/_search")
      .addParameter("size", "1000")
      .addParameter("scroll", "1m") // Hopefully more than enough to consume 1000 hits. TODO: add that as a parameter of the input format.
      .addParameter("preference", s"_shards:${currentSplit.shard};_only_node:${currentSplit.nodeId}")
      .build

    val req = new HttpPost(uri)
    req.setEntity(new StringEntity(query))

    val resp = executeHttpQuery(req)

    currentScrollWindowId = resp
        .get("_scroll_id")
        .asText()

    currentScrollWindowHits = resp
      .get("hits")
      .get("hits")
      .asInstanceOf[ArrayNode]

    nextRecordIndex = 0
  }

  override def reachedEnd() = {
    if (currentScrollWindowHits.size() > 0 && nextRecordIndex > currentScrollWindowHits.size() - 1)
      fetchNextScrollWindow()

    currentScrollWindowHits.size() == 0
  }

  override def nextRecord(reuse: T): T = {
    require(!reachedEnd(), "Already reached the end of the split.")

    val hit = currentScrollWindowHits.get(nextRecordIndex)
    nextRecordIndex += 1

    log.debug("Yielding new record for hit: {}", hit)

    parseHit(hit, reuse)
  }

  override def close() = {
    log.info("Closing split {}", currentSplit)

    val uri = new URIBuilder()
      .setScheme("http")
      .setHost(currentSplit.nodeHost)
      .setPort(currentSplit.nodePort)
      .setPath(s"/_search/scroll")
      .addParameter("scroll_id", currentScrollWindowId)
      .build

    try {
      executeHttpQuery(new HttpDelete(uri))
    }
    finally {
      currentScrollWindowId = null
      currentSplit = null
      currentScrollWindowHits = null
    }
  }

  private def fetchNextScrollWindow() = {
    val uri = new URIBuilder()
      .setScheme("http")
      .setHost(currentSplit.nodeHost)
      .setPort(currentSplit.nodePort)
      .setPath(s"/_search/scroll")
      .addParameter("scroll", "1m")
      .addParameter("scroll_id", currentScrollWindowId)
      .addParameter("preference", s"_shards:${currentSplit.shard};_only_node:${currentSplit.nodeId}")
      .build()

    val resp = executeHttpQuery(new HttpGet(uri))

    currentScrollWindowId = resp
      .get("_scroll_id")
      .asText()

    currentScrollWindowHits = resp
      .get("hits")
      .get("hits")
      .asInstanceOf[ArrayNode]

    nextRecordIndex = 0
  }

  private def getOnOneNode(path: String) = {
    val uri = new URIBuilder()
        .setScheme("http")
        .setHost(nodes.head) // TODO: retry on other unavailable nodes if it fails.
        .setPort(port)
        .setPath(path)
      .build

    new HttpGet(uri)
  }

  private def executeHttpQuery(request: HttpUriRequest): JsonNode = {
    val resp = httpClient.execute(request)
    jsonParser.readTree(resp.getEntity.getContent)
  }

  private def parseHit(json: JsonNode, reuse: T): T = {
    log.info("json: " + json)

    val fieldValues = json
      .get("fields")
      .asInstanceOf[ObjectNode]

    val parsedValues = queriedFields
      .map { case (fieldName, i) =>
        val cl = schema(i).fieldClass
        require(valueParsers.contains(cl), s"Read a ${cl.getCanonicalName} value from a Elasticsearch hit is not supported. Supported types are: $supportedTypes.")
        val value =
          if (fieldValues.has(fieldName))
            fieldValues.get(fieldName).asInstanceOf[ArrayNode].get(0)
          else
            null

        if (value == null)
          null
        else
          valueParsers(cl)(value)
      }

    deserializer.createOrReuseInstance(parsedValues, reuse)
  }

  // Note: for datetime, the client will have to get as a String, and do the parsing herself.
  private val valueParsers = Map[Class[_], JsonNode => AnyRef](
    (classOf[String], _.asText()),
    (classOf[Byte], n => new java.lang.Byte(n.asInt().toByte)),
    (classOf[Int], n => new java.lang.Integer(n.asInt())),
    (classOf[Long], n => new java.lang.Long(n.asLong())),
    (classOf[Float], n => new java.lang.Float(n.asDouble())),
    (classOf[Double], n => new java.lang.Double(n.asDouble())),
    (classOf[Boolean], n => new java.lang.Boolean(n.asBoolean())),
    (classOf[Array[Double]], n => n.asInstanceOf[ArrayNode].elements().asScala.map(_.asDouble()).toArray),
    (classOf[java.lang.Byte], n => new java.lang.Byte(n.asInt().toByte)),
    (classOf[java.lang.Integer], n => new java.lang.Integer(n.asInt())),
    (classOf[java.lang.Long], n => new java.lang.Long(n.asLong())),
    (classOf[java.lang.Float], n => new java.lang.Float(n.asDouble())),
    (classOf[java.lang.Double], n => new java.lang.Double(n.asDouble())),
    (classOf[java.lang.Boolean], n => new java.lang.Boolean(n.asBoolean())),
    (classOf[Array[java.lang.Double]], n => n.asInstanceOf[ArrayNode].elements().asScala.map(_.asDouble()).toArray)
  )

  private val esSimpleMappings = Map[String, Option[Class[_]]](
    "string" -> Some(classOf[String]),
    "ip" -> Some(classOf[String]),
    "integer" -> Some(classOf[java.lang.Integer]),
    "date" -> Some(classOf[String]),
    "completion" -> None, // Not supported
    "boolean" -> Some(classOf[java.lang.Boolean]),
    "long" -> Some(classOf[java.lang.Long]),
    "double" -> Some(classOf[java.lang.Double]),
    "float" -> Some(classOf[java.lang.Float]),
    "short" -> Some(classOf[java.lang.Integer]),
    "byte" -> Some(classOf[java.lang.Byte]),
    "binary" -> None, // Not supported
    "geo_point" -> Some(classOf[Array[java.lang.Double]]),
    "geo_shape" -> None // Not supported
  ).withDefaultValue(None)

  private val supportedTypes = valueParsers.keys.map(_.getCanonicalName).mkString(", ")
}
