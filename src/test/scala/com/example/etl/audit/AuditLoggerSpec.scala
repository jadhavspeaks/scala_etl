package com.example.etl.audit

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class AuditLoggerSpec extends AnyFlatSpec with Matchers {
  "AuditLogger object" should "be creatable" in {
    val logger = AuditLogger
    logger shouldNot be (null)
  }
}
