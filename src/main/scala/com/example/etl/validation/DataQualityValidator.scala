package com.example.etl.validation

import com.example.etl.config._ // Import all config rule types
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.slf4j.LoggerFactory

object DataQualityValidator {
  private val logger = LoggerFactory.getLogger(this.getClass)

  def applyDQRules(df: DataFrame, dqRules: List[DataQualityRule]): DataFrame = {
    if (dqRules.isEmpty) {
      logger.info("No DQ rules to apply. Returning original DataFrame.")
      return df.withColumn("dq_passed", lit(true)).withColumn("dq_failure_reasons", array().cast("array<string>"))
    }
    logger.info(s"Applying ${dqRules.size} DQ rules.")

    var currentDf = df
    val failureReasonExpressions = dqRules.map { rule =>
      val colName = rule.columnName
      val ruleCondition = rule match {
        case NotNullRule(cn, _) => col(cn).isNull
        case TypeCheckRule(cn, expectedSparkType, _) =>
          // This is a simplistic type check. Assumes column is already cast.
          // A more robust check might involve trying to cast and seeing if it becomes null
          // or comparing string representation if original type was string.
          // For now, this is more of a placeholder or relies on prior casting.
          // Let's assume if it's not the target type, it's a fail (difficult to check directly post-cast without original)
          // A better way: try_cast in Spark 3.0+ can be used: try_cast(col(cn).cast(StringType) as expectedSparkType).isNull and col(cn).isNotNull
          // For this iteration, we'll keep it simple: if schema mapping failed to cast, it might be null.
          // This rule is more effective if applied before strong typing or if we check metadata.
          // For this step, a NOT NULL check is more practical from this list.
          // We'll focus on NOT_NULL and LENGTH for this iteration of the DQ module.
          logger.warn(s"TYPE_CHECK for column '$cn' to '$expectedSparkType' is currently a conceptual check. Effective implementation requires more context or pre-cast validation.")
          lit(false) // Placeholder, effectively makes this rule pass always for now.

        case LengthCheckRule(cn, minLenOpt, maxLenOpt, _) =>
          val lenCol = length(col(cn))
          val minCheck = minLenOpt.map(min => lenCol < lit(min)).getOrElse(lit(false))
          val maxCheck = maxLenOpt.map(max => lenCol > lit(max)).getOrElse(lit(false))
          minCheck.or(maxCheck)

        case MaxLengthRule(cn, maxLength, _) => length(col(cn)) > lit(maxLength)

        // RegexRule and ValueSetRule will be implemented later
        case _: RegexRule => logger.warn(s"REGEX rule for $colName not yet implemented."); lit(false)
        case _: ValueSetRule => logger.warn(s"VALUE_SET rule for $colName not yet implemented."); lit(false)

        case _ => lit(false) // Default for any other rule type not yet handled
      }
      when(ruleCondition, lit(s"Column '$colName' failed ${rule.dqType} check.")).otherwise(lit(null).cast(StringType))
    }

    // Combine all failure reason expressions into a single array column
    val allFailureReasonsCol = array_remove(array(failureReasonExpressions: _*), lit(null).cast(StringType))

    currentDf = currentDf.withColumn("dq_failure_reasons", allFailureReasonsCol)
    currentDf = currentDf.withColumn("dq_passed", size(col("dq_failure_reasons")) === 0)

    val passedCount = currentDf.filter(col("dq_passed") === true).count()
    val failedCount = currentDf.filter(col("dq_passed") === false).count()
    logger.info(s"DQ validation complete. Passed rows: $passedCount, Failed rows: $failedCount")
    // For debugging:
    // currentDf.filter(col("dq_passed") === false).show(5, false)

    currentDf
  }
}
