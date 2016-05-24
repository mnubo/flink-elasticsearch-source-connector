package com.mnubo.flink.streaming.connectors

import java.util

import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.java.typeutils.runtime.TupleSerializerBase
import org.apache.flink.core.memory.{DataInputView, DataOutputView}
import org.apache.flink.types.NullFieldException

class DataRowSerializer(private val typeInfo: DataRowTypeInfo, private val elementSerializers: Array[TypeSerializer[_]]) extends TupleSerializerBase[DataRow](classOf[DataRow], elementSerializers) {
  require(typeInfo.getArity == elementSerializers.length, "typeInfo arity should be the same as elementSerializers")

  override def createInstance() =
    new DataRow(Array.fill[Any](typeInfo.getArity)(null), typeInfo)

  override def isImmutableType =
    false

  override def getLength =
    -1

  override def canEqual(obj: scala.Any) =
    obj.isInstanceOf[DataRowSerializer]

  override def equals(other: Any) =
    canEqual(other) && {
      val drs = other.asInstanceOf[DataRowSerializer]

      elementSerializers.sameElements(drs.elementSerializers) && typeInfo == drs.typeInfo
    }

  override def hashCode =
    31 * util.Arrays.hashCode(elementSerializers.asInstanceOf[Array[AnyRef]]) + typeInfo.hashCode()

  override def duplicate() = {
    val duplicatedFieldSerializers = elementSerializers.map(_.duplicate())

    if (duplicatedFieldSerializers.indices.exists(i => duplicatedFieldSerializers(i) != elementSerializers(i)))
      new DataRowSerializer(typeInfo, duplicatedFieldSerializers)
    else
      this
  }

  override def copy(from: DataRow): DataRow =
    new DataRow(
      from.data.zipWithIndex.map { case (elt, i) => elementSerializers(i).asInstanceOf[TypeSerializer[Any]].copy(elt) },
      typeInfo
    )

  override def copy(from: DataRow, reuse: DataRow): DataRow = {
    reuse
      .data
      .indices
      .foreach { i =>
        reuse.data(i) = elementSerializers(i).asInstanceOf[TypeSerializer[Any]].copy(from.data(i), reuse.data(i))
      }

    reuse
  }

  override def createInstance(fields: Array[AnyRef]): DataRow =
    new DataRow(
      fields.map(_.asInstanceOf[Any]),
      typeInfo
    )

  override def createOrReuseInstance(fields: Array[AnyRef], reuse: DataRow) = {
    reuse
      .data
      .indices
      .foreach { i =>
        reuse.data(i) = fields(i)
      }

    reuse
  }

  override def copy(source: DataInputView, target: DataOutputView) =
    elementSerializers.foreach(fs => fs.copy(source, target))

  override def serialize(record: DataRow, target: DataOutputView) =
    elementSerializers
      .indices
      .foreach { i =>
        try {
          elementSerializers(i)
            .asInstanceOf[TypeSerializer[Any]]
            .serialize(record.data(i), target)
        }
        catch {
          case npe: NullPointerException =>
            throw new NullFieldException(i, npe)
        }
      }

  override def deserialize(source: DataInputView) =
    new DataRow(
      elementSerializers.map(fs => fs.deserialize(source)),
      typeInfo
    )

  override def deserialize(reuse: DataRow, source: DataInputView) = {
    elementSerializers
      .indices
      .foreach { i =>
        reuse.data(i) = elementSerializers(i).asInstanceOf[TypeSerializer[Any]].deserialize(reuse.data(i), source)
      }

    reuse
  }
}
