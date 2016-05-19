package com.mnubo.flink.streaming.connectors

import org.scalatest.{Matchers, WordSpec}

class DataRowSpec extends WordSpec with Matchers {
  "A data row" should {
    "not accept schema size discrepency" in {
      an[IllegalArgumentException] shouldBe thrownBy(DataRow(Array(1, "abc"), classOf[Int]))
    }
    "not accept schema type discrepency" in {
      an[IllegalArgumentException] shouldBe thrownBy(DataRow(Array(1, "abc"), classOf[Int], classOf[WordSpec]))
    }
    "not accept null values in data array" in {
      an[IllegalArgumentException] shouldBe thrownBy(DataRow(Array(1, null)))
    }
    "not accept null arguments" in {
      an[IllegalArgumentException] shouldBe thrownBy(DataRow(null))
      an[IllegalArgumentException] shouldBe thrownBy(DataRow(null, classOf[Int]))
      an[IllegalArgumentException] shouldBe thrownBy(new DataRow(Array(1, "abc"), null))
      an[IllegalArgumentException] shouldBe thrownBy(new DataRow(null, new DataRowTypeInfo(Seq.empty, Seq.empty)))
    }
    "be equal to an other data row with the same values" in {
      DataRow(1, "abc") shouldEqual DataRow(1, "abc")
    }
    "hashCode should be equal to an other data row hashcode" in {
      DataRow(1, "abc").hashCode shouldEqual DataRow(1, "abc").hashCode
    }
    "not be equal to an other data row with different values" in {
      DataRow(2, "abc") should not equal DataRow(1, "abc")
    }
    "hashCode should not be equal to an other data row hashcode" in {
      DataRow(2, "abc").hashCode should not equal DataRow(1, "abc").hashCode
    }
    "allow to get values by index" in {
      val sut = DataRow(2, "abc")

      sut[Int](0) shouldEqual 2
      sut[String](1) shouldEqual "abc"
    }
    "allow to get values by name" in {
      val sut = DataRow(2, "abc")

      sut[Int]("0") shouldEqual 2
      sut[String]("1") shouldEqual "abc"
    }
    "implement Product" in {
      val sut = DataRow(2, "abc")

      sut.productElement(0) shouldEqual 2
      sut.productElement(1) shouldEqual "abc"
      sut.productArity shouldEqual 2
      sut.canEqual("a string") shouldBe false
      sut.canEqual(null.asInstanceOf[DataRow]) shouldBe false
      sut.canEqual(DataRow(1, "abc")) shouldBe true
    }
    "return a useful string representation" in {
      DataRow(2, "abc").toString shouldEqual "DataRow(2, abc)"
    }
  }
}
