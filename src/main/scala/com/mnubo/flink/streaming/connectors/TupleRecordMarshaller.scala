package com.mnubo.flink.streaming.connectors

import org.apache.commons.lang3.ClassUtils
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.java.typeutils.TupleTypeInfoBase
import org.apache.flink.api.java.typeutils.runtime.TupleSerializerBase

@SerialVersionUID(1L)
class TupleRecordMarshaller[T](typeInfo: TupleTypeInfoBase[T]) extends RecordMarshaller[T] {
  private val serializer = typeInfo
    .createSerializer(new ExecutionConfig)
    .asInstanceOf[TupleSerializerBase[T]]

  override def typeInformation =
    typeInfo

  override def configureFields(types: Seq[MarshallerFieldDescriptor]) = {
    require(
      typeInfo.getArity == types.length,
      s"The number of fields selected in the tuple of type ${typeInfo.getTypeClass.getName} (${typeInfo.getArity}) does not match the number of fields returned by the Dataset (${types.size})"
    )

    (0 until typeInfo.getArity)
      .foreach { i =>
        val recordFieldClass = typeInfo.getTypeAt(i).getTypeClass
        require(
          ClassUtils.isAssignable(
            recordFieldClass,
            types(i).fieldClass
          ),
          s"Tuple field $i of type ${recordFieldClass.getName} is not assignable from DataSet field $i of type ${types(i).fieldClass.getName}"
        )
      }
  }

  override def createOrReuseInstance(fields: Seq[AnyRef], reuse: T): T =
    serializer.createOrReuseInstance(fields.toArray, reuse)
}
