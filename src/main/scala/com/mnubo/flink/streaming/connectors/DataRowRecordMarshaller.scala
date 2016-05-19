package com.mnubo.flink.streaming.connectors

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor

@SerialVersionUID(1L)
class DataRowRecordMarshaller extends RecordMarshaller[DataRow] {
  private var typeInfo: DataRowTypeInfo = null

  @transient
  private lazy val serializer = typeInfo
    .createSerializer(new ExecutionConfig)
    .asInstanceOf[DataRowSerializer]

  override def typeInformation =
    typeInfo

  override def configureFields(types: Seq[MarshallerFieldDescriptor]) = {
    val (fieldNames, typeInfos): (Seq[String], Seq[TypeInformation[_]]) =
      types
        .map { tp =>
          tp.fieldName -> TypeExtractor.createTypeInfo(tp.fieldClass)
        }
        .unzip

    typeInfo = new DataRowTypeInfo(fieldNames, typeInfos)
  }

  override def createOrReuseInstance(fields: Seq[AnyRef], reuse: DataRow): DataRow =
    serializer.createOrReuseInstance(fields.toArray[AnyRef], reuse)
}
