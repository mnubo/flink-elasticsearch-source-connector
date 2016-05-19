package com.mnubo.flink.streaming.connectors

import java.util

import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.core.memory.{DataInputView, DataOutputView}
import org.apache.flink.types.NullFieldException

class DataRowSerializer(private val typeInfo: DataRowTypeInfo, private val fieldSerializers: Array[TypeSerializer[_]]) extends TypeSerializer[DataRow] {
  require(typeInfo.getArity == fieldSerializers.length, "typeInfo arity should be the same as fieldSerializers")

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

      fieldSerializers.sameElements(drs.fieldSerializers) && typeInfo == drs.typeInfo
    }

  override def hashCode =
    31 * util.Arrays.hashCode(fieldSerializers.asInstanceOf[Array[AnyRef]]) + typeInfo.hashCode()

  override def duplicate() = {
    val duplicatedFieldSerializers = fieldSerializers.map(_.duplicate())

    if (duplicatedFieldSerializers.indices.exists(i => duplicatedFieldSerializers(i) != fieldSerializers(i)))
      new DataRowSerializer(typeInfo, duplicatedFieldSerializers)
    else
      this
  }

  override def copy(from: DataRow) =
    new DataRow(
      from.data.zipWithIndex.map { case (elt, i) => fieldSerializers(i).asInstanceOf[TypeSerializer[Any]].copy(elt) },
      typeInfo
    )

  override def copy(from: DataRow, reuse: DataRow) = {
    reuse
      .data
      .indices
      .foreach { i =>
        reuse.data(i) = fieldSerializers(i).asInstanceOf[TypeSerializer[Any]].copy(from.data(i), reuse.data(i))
      }

    reuse
  }

  def createOrReuseInstance(fields: Seq[AnyRef], reuse: DataRow) = {
    reuse
      .data
      .indices
      .foreach { i =>
        reuse.data(i) = fields(i)
      }

    reuse
  }

  override def copy(source: DataInputView, target: DataOutputView) =
    fieldSerializers.foreach(fs => fs.copy(source, target))

  override def serialize(record: DataRow, target: DataOutputView) =
    fieldSerializers
      .indices
      .foreach { i =>
        try {
          fieldSerializers(i)
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
      fieldSerializers.map(fs => fs.deserialize(source)),
      typeInfo
    )

  override def deserialize(reuse: DataRow, source: DataInputView) = {
    fieldSerializers
      .indices
      .foreach { i =>
        reuse.data(i) = fieldSerializers(i).asInstanceOf[TypeSerializer[Any]].deserialize(reuse.data(i), source)
      }

    reuse
  }
}
