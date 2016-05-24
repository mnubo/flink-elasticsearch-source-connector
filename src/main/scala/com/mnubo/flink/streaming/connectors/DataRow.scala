package com.mnubo.flink.streaming.connectors

import org.apache.commons.lang3.ClassUtils
import org.apache.flink.api.java.typeutils.TypeExtractor
import scala.language.existentials

class DataRow(private [connectors] val data: Array[Any], info: DataRowTypeInfo) extends Product with Serializable {
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
    canEqual(that) && data.sameElements(that.asInstanceOf[DataRow].data) // Should it also have the same schema?

  override def hashCode = {
    var result = 1

    for (element <- data)
      result = 31 * result + (if (element == null) 0 else element.hashCode)

    result
  }

  override def toString =
    data
      .map(v => if (v == null) "null" else v.toString)
      .mkString("DataRow(", ", ", ")")
}

object DataRow {
  /**
    * Builds a DataRow, inferring the schema by looking at the given values.
    *
    * Nulls are not supported.
    */
  def apply(data: Any*): DataRow = {
    require(data != null, "data cannot be null")

    apply(data.toArray, data.map(_.getClass): _*)
  }

  /**
    * Builds a DataRow with the given schema.
    *
    * Nulls are supported.
    */
  def apply(data: Array[Any], types: Class[_]*): DataRow = {
    require(data != null, "data cannot be null")
    require(types != null, "types cannot be null")
    require(data.length == types.size, "data must have the same size as types")

    val names = data.indices.map(i => s"dr$i")

    val typeInfos = types.indices.map { i =>
      require(isAssignable(data(i), types(i)), s"data element $i '${data(i)}' is not compatible with class ${types(i).getName}")
      TypeExtractor.createTypeInfo(types(i))
    }

    new DataRow(data, new DataRowTypeInfo(names, typeInfos))
  }

  private def isAssignable(value: Any, cl: Class[_]) = {
    if (value == null && classOf[AnyRef].isAssignableFrom(cl))
      true
    else
      ClassUtils.isAssignable(value.getClass, cl)
  }
}