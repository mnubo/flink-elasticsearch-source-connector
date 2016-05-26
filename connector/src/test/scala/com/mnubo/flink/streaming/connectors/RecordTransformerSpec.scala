package com.mnubo.flink.streaming.connectors

import org.apache.flink.api.common.typeutils.CompositeType.InvalidFieldReferenceException
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.scalatest.{Matchers, WordSpec}

class RecordTransformerSpec extends WordSpec with Matchers {
  val typeInfo = DataRowTypeInfo(
    Seq("age", "name", "gender"),
    Seq(TypeExtractor.getForClass(classOf[Int]), TypeExtractor.getForClass(classOf[String]), TypeExtractor.getForClass(classOf[String]))
  )
  "A record transformer" should {
    "not accept no fields" in {
      an[IllegalArgumentException] shouldBe thrownBy(RecordTransformer.mapFields(typeInfo, Seq.empty:_*))
    }
    "not accept null arguments" in {
      an[IllegalArgumentException] shouldBe thrownBy(RecordTransformer.mapFields(null, ExistingField("*")))
      an[IllegalArgumentException] shouldBe thrownBy(RecordTransformer.mapFields(typeInfo, null))
    }
    "not accept duplicate field names" in {
      an[IllegalArgumentException] shouldBe thrownBy(RecordTransformer.mapFields(typeInfo, NewField("age", TypeExtractor.getForObject(1)), ExistingField("*")))
    }
    "not accept unknown field names" in {
      an[InvalidFieldReferenceException] shouldBe thrownBy(RecordTransformer.mapFields(typeInfo, ExistingField("unknonw")))
    }

    "transform" which {
      val baseFields = Seq(Value(3, "age"), Value("bob", "name"), Value("male", "gender"))
      val base = DataRow(baseFields:_*)
      "work with specific names" in {
        val transformer = RecordTransformer.mapFields(typeInfo, ExistingField("age"), ExistingField("gender"))
        val actual = transformer.transform(base)
        val expected = DataRow(Value(3, "age"), Value("male", "gender"))
        actual shouldEqual expected
      }

      "work specific names with a different order" in {
        val transformer = RecordTransformer.mapFields(typeInfo, ExistingField("gender"), ExistingField("age"))
        val actual = transformer.transform(base)
        val expected = DataRow(Value("male", "gender"), Value(3, "age"))
        actual shouldEqual expected
      }

      "work with *" in {
        val transformer = RecordTransformer.mapFields(typeInfo, ExistingField("*"))
        val actual = transformer.transform(base)
        actual shouldEqual base
      }

      "work with _" in {
        val transformer = RecordTransformer.mapFields(typeInfo, ExistingField("_"))
        val actual = transformer.transform(base)
        actual shouldEqual base
      }

      "work with mixed values" in {
        val transformer = RecordTransformer.mapFields(typeInfo, NewField("hair color", TypeExtractor.getForObject("")), ExistingField("age"))
        val actual = transformer.transform(base, "brown")
        val expected = DataRow(Value("brown", "hair color"), Value(3, "age"))
        actual shouldEqual expected
      }

      "work with mixed values and all records" in {
        val transformer = RecordTransformer.mapFields(typeInfo, NewField("hair color", TypeExtractor.getForObject("")), ExistingField("*"), NewField("style", TypeExtractor.getForObject(0)))
        val actual = transformer.transform(base, "brown", 4)
        val expected = DataRow(Seq(Value("brown", "hair color")) ++ baseFields ++ Seq(Value(4, "style")):_*)
        actual shouldEqual expected
      }

      "work with aggregation" in {
        val dr1 = DataRow(Value(3, "age"), Value("bob", "name"), Value("male", "gender"))
        val dr2 = DataRow(Value(16, "age"), Value("alice", "name"), Value("female", "gender"))
        val dr3 = DataRow(Value(1, "age"), Value("ary", "name"), Value("male", "gender"))

        val ds = DataRowTestHelper.fromElements(dr1, dr2, dr3)

        val transformer = RecordTransformer.mapFields(dr1.info, ExistingField("age"), ExistingField("gender"))
        val actual = ds.groupBy("gender")
          .sum(0)
          .map(transformer.transform(_))(transformer.typeInfo, transformer.classTag)
          .collect()

        val expected = Seq(
          DataRow(Value(4, "age"), Value("male", "gender")),
          DataRow(Value(16, "age"), Value("female", "gender"))
        )
        actual shouldEqual expected
      }

      "work with nesting" in {
        val dr1 = DataRow(Value(3, "age"), Value("bob", "name"), Value("male", "gender"))
        val dr2 = DataRow(Value(16, "age"), Value("alice", "name"), Value("female", "gender"))
        val dr3 = DataRow(Value(1, "age"), Value("ary", "name"), Value("male", "gender"))

        val ds = DataRowTestHelper.fromElements(dr1, dr2, dr3)

        val transformer = RecordTransformer.mapFields(dr1.info, ExistingField("age"), ExistingField("gender"))
        val actual = ds.groupBy("gender")
          .sum(0)
          .map(transformer.transform(_))(transformer.typeInfo, transformer.classTag)
          .collect()

        val expected = Seq(
          DataRow(Value(4, "age"), Value("male", "gender")),
          DataRow(Value(16, "age"), Value("female", "gender"))
        )
        actual shouldEqual expected
      }
    }
  }
}
