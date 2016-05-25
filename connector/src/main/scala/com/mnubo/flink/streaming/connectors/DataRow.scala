package com.mnubo.flink.streaming.connectors

import org.apache.commons.lang3.ClassUtils
import org.apache.flink.api.common.operators.Keys.ExpressionKeys._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.CompositeType.InvalidFieldReferenceException
import org.apache.flink.api.java.typeutils.TypeExtractor

import scala.language.existentials

sealed trait FieldSpecification

case object AllFields extends FieldSpecification

case class Field(name: String) extends FieldSpecification

case class Value(v: Any, name: String, givenTypeInfo: Option[TypeInformation[_]] = None) extends FieldSpecification {
  require(v != null || givenTypeInfo.isDefined, "You must pass a TypeInformation for null values")

  val typeInfo = givenTypeInfo match {
    case Some(ti) => ti
    case None => TypeExtractor.getForObject(v)
  }

  require(isAssignable(v, typeInfo.getTypeClass), s"data element '$v' is not compatible with class ${typeInfo.getTypeClass.getName}")

  private def isAssignable(value: Any, cl: Class[_]) = {
    if (value == null && classOf[AnyRef].isAssignableFrom(cl))
      true
    else
      ClassUtils.isAssignable(value.getClass, cl)
  }
}

object Value {
  def apply(v: Any, name: String, givenTypeInfo: TypeInformation[_]) = {
    new Value(v, name, Some(givenTypeInfo))
  }
}



class DataRow(private [connectors] val data: Array[Any], private [connectors] val info: DataRowTypeInfo) extends Product with Serializable {
  require(data != null, "data must not be null")
  require(info != null, "info must not be null")
  require(data.length == info.getArity, "data must be of the correct arity")

  def apply[T](i: Int): T =
    data(i).asInstanceOf[T]

  def apply[T](fieldExpression: String): T =
    apply(info.getFieldIndex(fieldExpression))

  override def productElement(n: Int): Any =
    apply[AnyRef](n)

  override def productArity =
    info.getArity

  override def canEqual(that: Any) =
    that.isInstanceOf[DataRow]

  override def equals(that: Any) =
    canEqual(that) && data.sameElements(that.asInstanceOf[DataRow].data) && info.getFieldNames.sameElements(that.asInstanceOf[DataRow].info.getFieldNames)

  override def hashCode = {
    var result = 1

    for (element <- data)
      result = 31 * result + (if (element == null) 0 else element.hashCode)

    result
  }

  override def toString =
    info.getFieldNames
      .zip(data.map(v => if (v == null) "null" else v.toString))
      .map{case (name, value) => s"$name=$value"}
      .mkString("DataRow(", ", ", ")")
}

object DataRow {
  /**
    * Builds a DataRow.
    */
  def apply(data: Value*): DataRow = {
    require(data != null, "data cannot be null")
    require(!data.contains(null), "data value cannot be null")
    require(data.length == data.map(_.name).distinct.length, s"a name can be used only once. names were ${data.map(_.name)}")

    new DataRow(
      data.map(_.v).toArray,
      new DataRowTypeInfo(
        data.map(_.name),
        data.map(_.typeInfo)
      )
    )
  }
}