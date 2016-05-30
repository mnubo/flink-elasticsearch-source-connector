package com.mnubo.flink.streaming.connectors

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}

import scala.reflect.ClassTag

object DataRowTestHelper {
  def fromElements(rows:DataRow*)(implicit executionEnvironment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment, typeInfo:DataRowTypeInfo = null) : DataSet[DataRow] = {
    require(rows != null, "rows must not be null")
    require(rows.nonEmpty ||  typeInfo != null, "rows must not be null")
    val actualTypeInfo = if(typeInfo != null) typeInfo else rows.head.info
    executionEnvironment.fromElements[DataRow](rows:_*)(ClassTag[DataRow](classOf[DataRow]), actualTypeInfo)
  }
}
