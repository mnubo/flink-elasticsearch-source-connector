package com.mnubo.flink.streaming.connectors

import java.lang.reflect.Field

import org.apache.commons.lang3.ClassUtils
import org.apache.flink.api.java.typeutils.PojoTypeInfo

import scala.annotation.tailrec

@SerialVersionUID(1L)
class PojoRecordMarshaller[T](typeInfo: PojoTypeInfo[T], fieldNames: Array[String]) extends RecordMarshaller[T] {
  private val typeClass = typeInfo.getTypeClass

  fieldNames.foreach { name =>
    require(typeInfo.getFieldIndex(name) >= 0, s"$name is not part of POJO type ${typeClass.getCanonicalName}")
  }

  @transient
  private lazy val typeFields = {
    val nameToField = findAllFieldsIn(typeClass)
    fieldNames.map(nameToField)
  }

  override def typeInformation =
    typeInfo

  override def configureFields(types: Seq[MarshallerFieldDescriptor]) = {
    require(
      types.size == typeFields.length,
      s"The number of fields selected in the POJO ${typeClass.getCanonicalName} (${typeFields.length}) does not match the number of fields returned by the Dataset (${types.size})"
    )

    fieldNames
      .indices
      .foreach { i =>
        val recordFieldClass = typeInfo.getTypeAt(typeInfo.getFieldIndex(fieldNames(i))).getTypeClass

        require(
          ClassUtils.isAssignable(
            recordFieldClass,
            types(i).fieldClass
          ),
          s"Record field $i of type ${recordFieldClass.getName} is not assignable from DataSet field $i of type ${types(i).fieldClass.getName}"
        )
      }
  }

  override def createOrReuseInstance(fieldValues: Seq[AnyRef], reuse: T): T = {
    fieldValues
      .zip(typeFields)
      .foreach { case (value, field) =>
        field.set(reuse, value)
      }

    reuse
  }

  @tailrec
  private def findAllFieldsIn(clasz: Class[_], acc: Map[String, Field] = Map.empty): Map[String, Field] =
    if (clasz == null)
      acc
    else {
      val newFields = clasz.getFields.map(f => f.getName -> f)
      findAllFieldsIn(clasz.getSuperclass, acc ++ newFields)
    }

}
