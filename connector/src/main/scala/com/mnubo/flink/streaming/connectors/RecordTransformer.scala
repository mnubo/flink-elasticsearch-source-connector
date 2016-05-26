package com.mnubo.flink.streaming.connectors

import org.apache.flink.api.common.operators.Keys.ExpressionKeys._
import org.apache.flink.api.common.typeinfo.TypeInformation

import scala.annotation.tailrec
import scala.language.existentials
import scala.reflect.ClassTag

sealed trait FieldSpecification extends Serializable

case class ExistingField(name: String) extends FieldSpecification

case class NewField(name: String, typeInfo: TypeInformation[_]) extends FieldSpecification

trait RecordTransformer extends Serializable {
  val classTag = ClassTag[DataRow](classOf[DataRow])
  def typeInfo : DataRowTypeInfo
  def transform(dataRow: DataRow, values:Any*) : DataRow
}

class FieldMapperRecordTransformer private[connectors](srcTypeInfo:DataRowTypeInfo, fieldSpecifications: FieldSpecification*) extends RecordTransformer {
  require(srcTypeInfo != null, s"srcTypeInfo must not be null")
  require(fieldSpecifications != null, s"fieldSpecifications must not be null")
  require(fieldSpecifications.nonEmpty, s"fieldSpecifications must not be empty")
  require(!fieldSpecifications.contains(null), s"fieldSpecifications must not contain any nulls")

  override val typeInfo = {
    val (fieldNames, elementTypes) = fieldSpecifications.flatMap {
      case ExistingField(name) if name == SELECT_ALL_CHAR || name == SELECT_ALL_CHAR_SCALA => srcTypeInfo.getFieldNames.zip(srcTypeInfo.getElementTypes)
      case ExistingField(name) => Seq(name -> srcTypeInfo.getFieldType(name))
      case NewField(name, newFieldTypeInfo) => Seq(name -> newFieldTypeInfo)
    }.unzip
    require(fieldNames.length == fieldNames.distinct.length, s"Fields can't have duplicates. Fields were $fieldNames.")
    new DataRowTypeInfo(fieldNames, elementTypes)
  }

  private def newFieldsNames = fieldSpecifications.collect{ case newValue: NewField => newValue.name }

  override def transform(dataRow: DataRow, values:Any*) : DataRow = {
    require(dataRow != null, s"dataRow must not be null")
    require(values != null, s"values must not be null")
    require(newFieldsNames.length == values.length, s"Must specify values for all new fields and only new fields. New fields are '$newFieldsNames'")

    val resultValues = new Array[Any](typeInfo.getArity)
    @tailrec
    def transform(index:Int, remainingSpecs: Seq[FieldSpecification], remainingValues:Seq[Any]) : DataRow = {
      if(remainingSpecs.isEmpty) {
        new DataRow(resultValues, typeInfo)
      } else {
        val currentSpec = remainingSpecs.head
        currentSpec match {
          case ExistingField(name) if name == SELECT_ALL_CHAR || name == SELECT_ALL_CHAR_SCALA =>
            Array.copy(dataRow.data, 0, resultValues, index, dataRow.data.length)
            transform(index + dataRow.data.length, remainingSpecs.tail, remainingValues)
          case ExistingField(name) =>
            resultValues(index) = dataRow(name)
            transform(index + 1, remainingSpecs.tail, remainingValues)
          case NewField(name, _) =>
            resultValues(index) = remainingValues.head
            transform(index + 1, remainingSpecs.tail, remainingValues.tail)
        }
      }
    }
    transform(0, fieldSpecifications, values)
  }
}

object RecordTransformer {
  def mapFields(srcTypeInfo: DataRowTypeInfo, fieldSpecifications: FieldSpecification*) : RecordTransformer = {
    new FieldMapperRecordTransformer(srcTypeInfo, fieldSpecifications:_*)
  }
}
