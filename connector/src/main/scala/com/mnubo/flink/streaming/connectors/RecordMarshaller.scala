package com.mnubo.flink.streaming.connectors

import org.apache.flink.api.common.typeinfo.TypeInformation
import scala.language.existentials

/**
  * An interface helping datasources to convert emmitted records from an internal list of values to the desired type T.
  */
trait RecordMarshaller[T] extends Serializable {
  /**
    * The datasource will call this method before emmiting records. It will give a chance to the marshaller either to
    * configure itself for accepting this sequence of name / type, or to check that the given sequence is compatible
    * with type T.
    *
    * @param fieldDescriptors The list of fields the datasource will emmit.
    */
  def configureFields(fieldDescriptors: Seq[MarshallerFieldDescriptor])

  /**
    * The datasource will call this method each time a new record is available. It expect the marshaller to convert
    * the list of values to an instance of type T. If T is not immutable, the marshaller is expected to mutate the reuse
    * instance with the given field values. If T is mutable, the marshaller is expected to create an instance of its own.
    *
    * @param fieldValues the values emmited by the datasource
    * @param reuse if not immutable, the instance to set values in and return
    * @return the record of type T filled with the given values
    */
  def createOrReuseInstance(fieldValues: Seq[AnyRef], reuse: T): T

  /**
    * The underlying type information.
    */
  def typeInformation: TypeInformation[T]

}

case class MarshallerFieldDescriptor(fieldName: String, fieldClass: Class[_])

