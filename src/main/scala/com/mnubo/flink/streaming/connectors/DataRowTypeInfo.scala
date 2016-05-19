package com.mnubo.flink.streaming.connectors

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.CompositeType.TypeComparatorBuilder
import org.apache.flink.api.common.typeutils.{TypeComparator, TypeSerializer}
import org.apache.flink.api.java.typeutils.TupleTypeInfoBase
import org.apache.flink.api.scala.typeutils.CaseClassComparator

import scala.collection.mutable.ArrayBuffer

case class DataRowTypeInfo(private val fieldNames: Seq[String], private val elementTypes: Seq[TypeInformation[_]]) extends TupleTypeInfoBase(classOf[DataRow], elementTypes: _*) {
  override val getFieldNames =
    fieldNames.toArray

  override def getFieldIndex(fieldName: String) =
    fieldNames.indexOf(fieldName)

  override def createSerializer(config: ExecutionConfig): TypeSerializer[DataRow] =
    new DataRowSerializer(this, elementTypes.map(_.createSerializer(config)).toArray)

  protected def createTypeComparatorBuilder: TypeComparatorBuilder[DataRow] =
    new DataRowTypeComparatorBuilder

  // Copy of CaseClassTypeInfo#CaseClassTypeComparatorBuilder
  private class DataRowTypeComparatorBuilder extends TypeComparatorBuilder[DataRow] {
    val fieldComparators = ArrayBuffer.empty[TypeComparator[_]]
    val logicalKeyFields = ArrayBuffer.empty[Int]

    override def initializeTypeComparatorBuilder(size: Int) = {
      fieldComparators.sizeHint(size)
      logicalKeyFields.sizeHint(size)
    }

    override def addComparatorField(fieldId: Int, comparator: TypeComparator[_]) = {
      fieldComparators += comparator
      logicalKeyFields += fieldId
    }

    override def createTypeComparator(config: ExecutionConfig): TypeComparator[DataRow] =
      new CaseClassComparator[DataRow]( // Really, more a ProductComparator
        logicalKeyFields.toArray,
        fieldComparators.toArray,
        types.take(logicalKeyFields.max + 1).map(_.createSerializer(config))
      )
  }


}
