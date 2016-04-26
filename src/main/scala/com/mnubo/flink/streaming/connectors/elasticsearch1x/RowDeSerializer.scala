package com.mnubo.flink.streaming.connectors.elasticsearch1x

trait RowDeSerializer[T] extends Serializable {
  def expectedTypes: Seq[Class[_]]
  def createOrReuseInstance(fields: Seq[AnyRef], reuse: T): T
}
