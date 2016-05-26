package com.mnubo.flink.streaming.connectors

import org.apache.flink.api.java.typeutils.TypeExtractor
import org.scalatest.{Matchers, WordSpec}

class DataRowSpec extends WordSpec with Matchers {
  "A data row" should {
    "not accept duplicate field names" in {
      an[IllegalArgumentException] shouldBe thrownBy(DataRow(Value(1, "age"), Value("abc", "age")))
      an[IllegalArgumentException] shouldBe thrownBy(DataRowTypeInfo(Seq("age", "age"), Seq(TypeExtractor.getForClass(classOf[String]), TypeExtractor.getForClass(classOf[String]))))
    }
    "not accept null arguments" in {
      an[IllegalArgumentException] shouldBe thrownBy(DataRow(Value(null, "some_field")))
      an[IllegalArgumentException] shouldBe thrownBy(DataRow(null))
    }
    "be equal to an other data row with the same values" in {
      DataRow(Value(1, "age"), Value("abc", "name")) shouldEqual DataRow(Value(1, "age"), Value("abc", "name"))
    }
    "hashCode should be equal to an other data row hashcode" in {
      DataRow(Value(1, "age"), Value("abc", "name")).hashCode shouldEqual DataRow(Value(1, "age"), Value("abc", "name")).hashCode
    }
    "not be equal to an other data row with different values" in {
      DataRow(Value(2, "age"), Value("abc", "name")) should not equal DataRow(Value(1, "age"), Value("abc", "name"))
    }
    "hashCode should not be equal to an other data row hashcode" in {
      DataRow(Value(2, "age"), Value("abc", "name")).hashCode should not equal DataRow(Value(1, "age"), Value("abc", "name")).hashCode
    }
    "allow to get values by index" in {
      val sut = DataRow(Value(2, "age"), Value("abc", "name"))

      sut[Int](0) shouldEqual 2
      sut[String](1) shouldEqual "abc"
    }
    "allow to get values by name" in {
      val sut = DataRow(Value(2, "age"), Value("abc", "name"))

      sut[Int]("age") shouldEqual 2
      sut[String]("name") shouldEqual "abc"
    }
    "implement Product" in {
      val sut = DataRow(Value(2, "age"), Value("abc", "name"))

      sut.productElement(0) shouldEqual 2
      sut.productElement(1) shouldEqual "abc"
      sut.productArity shouldEqual 2
      sut.canEqual("a string") shouldBe false
      sut.canEqual(null.asInstanceOf[DataRow]) shouldBe false
      sut.canEqual(DataRow(Value(1, "age"), Value("abc", "name"))) shouldBe true
    }
    "return a useful string representation" in {
      DataRow(Value(2, "age"), Value("abc", "name")).toString shouldEqual "DataRow(age=2, name=abc)"
    }
  }
}
