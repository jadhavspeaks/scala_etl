package com.example.etl.config

// Schema Mapping
case class SchemaMappingRule(
    sourceName: String,
    targetName: String,
    targetType: String, // Spark SQL type string e.g. "StringType", "IntegerType", "DecimalType(10,2)"
    nullable: Option[Boolean] = Some(true),
    maxLength: Option[Int] = None, // For StringType
    format: Option[String] = None // For date/timestamp types
)

// Data Quality Rules
sealed trait DataQualityRule {
    def columnName: String
    def dqType: String
}

case class NotNullRule(columnName: String, dqType: String = "NOT_NULL") extends DataQualityRule
case class RegexRule(columnName: String, pattern: String, dqType: String = "REGEX") extends DataQualityRule
case class TypeCheckRule(columnName: String, expectedSparkType: String, dqType: String = "TYPE_CHECK") extends DataQualityRule
case class ValueSetRule(columnName: String, allowedValues: List[String], dqType: String = "VALUE_SET") extends DataQualityRule
case class LengthCheckRule(columnName: String, minLength: Option[Int] = None, maxLength: Option[Int] = None, dqType: String = "LENGTH_CHECK") extends DataQualityRule {
    require(minLength.isDefined || maxLength.isDefined, "Either minLength or maxLength must be defined for LengthCheckRule")
}
// Simple MaxLength only rule for convenience
case class MaxLengthRule(columnName: String, maxLength: Int, dqType: String = "MAX_LENGTH") extends DataQualityRule


// JSON Protocol for Spray JSON
import spray.json._
object JsonConfigProtocol extends DefaultJsonProtocol {
  implicit val schemaMappingRuleFormat = jsonFormat6(SchemaMappingRule)

  // For DataQualityRule (sealed trait)
  implicit object DataQualityRuleFormat extends RootJsonFormat[DataQualityRule] {
    def write(obj: DataQualityRule): JsValue = obj match {
      case r: NotNullRule => JsObject("columnName" -> JsString(r.columnName), "dqType" -> JsString("NOT_NULL"))
      case r: RegexRule => JsObject("columnName" -> JsString(r.columnName), "dqType" -> JsString("REGEX"), "pattern" -> JsString(r.pattern))
      case r: TypeCheckRule => JsObject("columnName" -> JsString(r.columnName), "dqType" -> JsString("TYPE_CHECK"), "expectedSparkType" -> JsString(r.expectedSparkType))
      case r: ValueSetRule => JsObject("columnName" -> JsString(r.columnName), "dqType" -> JsString("VALUE_SET"), "allowedValues" -> r.allowedValues.toJson)
      case r: LengthCheckRule =>
        val fields = scala.collection.mutable.ListBuffer[(String, JsValue)]()
        fields += "columnName" -> JsString(r.columnName)
        fields += "dqType" -> JsString("LENGTH_CHECK")
        r.minLength.foreach(ml => fields += "minLength" -> JsNumber(ml))
        r.maxLength.foreach(ml => fields += "maxLength" -> JsNumber(ml))
        JsObject(fields.toMap)
      case r: MaxLengthRule => JsObject("columnName" -> JsString(r.columnName), "dqType" -> JsString("MAX_LENGTH"), "maxLength" -> JsNumber(r.maxLength))
    }

    def read(json: JsValue): DataQualityRule = {
      val fields = json.asJsObject.getFields("columnName", "dqType") match {
        case Seq(JsString(colName), JsString(dqRuleType)) => (colName, dqRuleType)
        case _ => throw DeserializationException("DataQualityRule expected fields: columnName and dqType as strings")
      }
      val colName = fields._1
      val dqRuleType = fields._2

      json.asJsObject.getFields("dqType") match {
        case Seq(JsString("NOT_NULL")) => NotNullRule(colName)
        case Seq(JsString("REGEX")) => json.convertTo[RegexRule](jsonFormat(RegexRule, "columnName", "pattern", "dqType"))
        case Seq(JsString("TYPE_CHECK")) => json.convertTo[TypeCheckRule](jsonFormat(TypeCheckRule, "columnName", "expectedSparkType", "dqType"))
        case Seq(JsString("VALUE_SET")) => json.convertTo[ValueSetRule](jsonFormat(ValueSetRule, "columnName", "allowedValues", "dqType"))
        case Seq(JsString("LENGTH_CHECK")) => json.convertTo[LengthCheckRule](jsonFormat(LengthCheckRule, "columnName", "minLength", "maxLength", "dqType"))
        case Seq(JsString("MAX_LENGTH")) => json.convertTo[MaxLengthRule](jsonFormat(MaxLengthRule, "columnName", "maxLength", "dqType"))
        case _ => throw DeserializationException(s"Unknown DataQualityRule dqType: $dqRuleType")
      }
    }
  }
}
