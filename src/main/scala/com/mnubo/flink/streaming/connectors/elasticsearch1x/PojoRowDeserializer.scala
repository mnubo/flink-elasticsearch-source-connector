package com.mnubo.flink.streaming.connectors.elasticsearch1x

import java.lang.reflect.Field

import org.apache.flink.api.java.typeutils.PojoTypeInfo

import scala.annotation.tailrec

@SerialVersionUID(1L)
class PojoRowDeserializer[T](typeInfo: PojoTypeInfo[T], fieldNames: Array[String]) extends RowDeSerializer[T] {
  private val typeClass = typeInfo.getTypeClass

  fieldNames.foreach { name =>
    require(typeInfo.getFieldIndex(name) >= 0, s"$name is not part of POJO type ${typeClass.getCanonicalName}")
  }

  private val typeFields = {
    val nameToField = findAllFieldsIn(typeClass)
    fieldNames.map(nameToField)
  }

  override val expectedTypes: Seq[Class[_]] =
    fieldNames.map(name => typeInfo.getTypeAt(typeInfo.getFieldIndex(name)).getTypeClass)

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
