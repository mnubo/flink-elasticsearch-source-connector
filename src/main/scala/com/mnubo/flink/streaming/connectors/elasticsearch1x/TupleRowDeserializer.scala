package com.mnubo.flink.streaming.connectors.elasticsearch1x

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.java.typeutils.TupleTypeInfoBase
import org.apache.flink.api.java.typeutils.runtime.TupleSerializerBase

@SerialVersionUID(1L)
class TupleRowDeserializer[T](typeInfo: TupleTypeInfoBase[T]) extends RowDeSerializer[T] {
  private val serializer = typeInfo
    .createSerializer(new ExecutionConfig)
    .asInstanceOf[TupleSerializerBase[T]]

  override val expectedTypes: Seq[Class[_]] =
    (0 until typeInfo.getArity).map(i => typeInfo.getTypeAt(i).getTypeClass)

  override def createOrReuseInstance(fields: Seq[AnyRef], reuse: T): T =
    serializer.createOrReuseInstance(fields.toArray, reuse)
}
