package com.mnubo.flink.streaming.connectors

import java.util.regex.Pattern
import java.util.{List => JList}

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.operators.Keys.ExpressionKeys._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.CompositeType.{FlatFieldDescriptor, InvalidFieldReferenceException, TypeComparatorBuilder}
import org.apache.flink.api.common.typeutils.{CompositeType, TypeComparator, TypeSerializer}
import org.apache.flink.api.java.typeutils.TupleTypeInfoBase
import org.apache.flink.api.scala.typeutils.CaseClassComparator

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer

object DataRowTypeInfo {
  // Notes:
  // \p{L} is the POSIX regex class of characters 'unicode letter'.
  // identifiers are composed of segments separated by a '.'.
  // each segment is composed of letters, digits, '_', and '$', but they cannot start with a digit.
  // alternatively, instead of a string of segments, the wildcards '_' or '*' alone can be used.
  protected[connectors] val PATTERN_NESTED_FIELDS_WILDCARD =
    Pattern.compile("""[\p{L}_\$][\p{L}\p{Digit}_\$]*(\..+)?|\""" + SELECT_ALL_CHAR + """|\""" + SELECT_ALL_CHAR_SCALA)

}
case class DataRowTypeInfo(private val fieldNames: Seq[String], private val elementTypes: Seq[TypeInformation[_]]) extends TupleTypeInfoBase(classOf[DataRow], elementTypes: _*) {
  require(fieldNames != null, "fieldNames must not be null")
  require(elementTypes != null, "elementTypes must not be null")
  require(fieldNames.size == elementTypes.size, "fieldNames and elementTypes must have the same size")
  require(fieldNames.size == fieldNames.distinct.size, s"a name can be used only once. names were $fieldNames")

  import DataRowTypeInfo._

  override def getFlatFields(fieldExpression: String, offset: Int, result: JList[FlatFieldDescriptor]) = {
    val matcher = PATTERN_NESTED_FIELDS_WILDCARD.matcher(fieldExpression)

    if (!matcher.matches)
      throw new InvalidFieldReferenceException(s"""Invalid data row field reference "$fieldExpression".""")

    val field = matcher.group(0)

    if ((field == SELECT_ALL_CHAR) || (field == SELECT_ALL_CHAR_SCALA)) {
      var keyPosition: Int = 0
      for (fType <- elementTypes) {
        fType match {
          case ct: CompositeType[_] =>
            ct.getFlatFields(SELECT_ALL_CHAR, offset + keyPosition, result)
            keyPosition += ct.getTotalFields - 1
          case _ =>
            result.add(new FlatFieldDescriptor(offset + keyPosition, fType))
        }

        keyPosition += 1
      }
    }
    else {
      @tailrec
      def extractFlatFields(scanningIndex: Int, insertionPosition: Int): Unit =
        if (scanningIndex >= fieldNames.size)
          throw new InvalidFieldReferenceException(s"""Unable to find field "$field" in type $this.""")
        else {
          val scannedFieldName = fieldNames(scanningIndex)

          // Some field names can themselves have periods in their names.
          if (field == scannedFieldName || field.startsWith(scannedFieldName + ".")) {
            // found field

            val tail =
              if (field == scannedFieldName)
                null
              else
                fieldExpression.substring(scannedFieldName.length + 1)

            elementTypes(scanningIndex) match {
              case ct: CompositeType[_] =>
                ct.getFlatFields(if (tail == null) "*" else tail, insertionPosition, result)
              case _ if tail == null =>
                result.add(new FlatFieldDescriptor(insertionPosition, elementTypes(scanningIndex)))
              case _ =>
                throw new InvalidFieldReferenceException(s"""Nested field expression "$tail" not possible on atomic type ${elementTypes(scanningIndex)}.""")
            }
          }
          else
            extractFlatFields(scanningIndex + 1, insertionPosition + elementTypes(scanningIndex).getTotalFields)
        }

      extractFlatFields(0, offset)
    }
  }

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
      new CaseClassComparator[DataRow](// Really, more a ProductComparator
        logicalKeyFields.toArray,
        fieldComparators.toArray,
        elementTypes.take(logicalKeyFields.max + 1).map(_.createSerializer(config)).toArray
      )
  }
}
