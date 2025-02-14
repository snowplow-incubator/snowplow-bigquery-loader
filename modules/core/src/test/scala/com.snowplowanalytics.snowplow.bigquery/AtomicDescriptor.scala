/**
 * Copyright (c) 2013-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd., under the terms of the Snowplow
 * Limited Use License Agreement, Version 1.1 located at
 * https://docs.snowplow.io/limited-use-license-1.1 BY INSTALLING, DOWNLOADING, ACCESSING, USING OR
 * DISTRIBUTING ANY PORTION OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.bigquery

import com.google.protobuf.{DescriptorProtos, Descriptors}

/**
 * Mocks the protobuf Descriptor that would be returned by a BigQuery Writer for the Snowplow events
 * table
 */
object AtomicDescriptor {

  /** The initial descriptor altering table for self-describing entities */
  def initial: Descriptors.Descriptor = {
    val descriptorProto = DescriptorProtos.DescriptorProto.newBuilder
      .addField(0, eventId.setNumber(1)) // For laziness, only adding one atomic field
    fromDescriptorProtoBuilder(descriptorProto)
  }

  /** A table which has been altered to add the web_page unstruct event column */
  def withWebPage(legacyColumns: Boolean): Descriptors.Descriptor = {
    val descriptorProto = DescriptorProtos.DescriptorProto.newBuilder
      .addField(0, eventId.setNumber(1))
      .addField(1, webPage(legacyColumns).setNumber(2))
      .addNestedType(webPageNestedType)
    fromDescriptorProtoBuilder(descriptorProto)
  }

  def withTestUnstruct100(legacyColumns: Boolean): Descriptors.Descriptor = {
    val descriptorProto = DescriptorProtos.DescriptorProto.newBuilder
      .addField(0, eventId.setNumber(1))
      .addField(1, testUnstruct(legacyColumns, 0).setNumber(2))
      .addNestedType(testNestedType100)
    fromDescriptorProtoBuilder(descriptorProto)
  }

  def withTestUnstruct101(legacyColumns: Boolean): Descriptors.Descriptor = {
    val descriptorProto = DescriptorProtos.DescriptorProto.newBuilder
      .addField(0, eventId.setNumber(1))
      .addField(1, testUnstruct(legacyColumns, 1).setNumber(2))
      .addNestedType(testNestedType101)
    fromDescriptorProtoBuilder(descriptorProto)
  }

  def withTestUnstruct100And101: Descriptors.Descriptor = {
    val descriptorProto = DescriptorProtos.DescriptorProto.newBuilder
      .addField(0, eventId.setNumber(1))
      .addField(1, testUnstruct(legacyColumns = true, 0).setNumber(2))
      .addField(2, testUnstruct(legacyColumns = true, 1).setNumber(3))
      .addNestedType(testNestedType100)
      .addNestedType(testNestedType101)
    fromDescriptorProtoBuilder(descriptorProto)
  }

  def withTestContext100(legacyColumns: Boolean): Descriptors.Descriptor = {
    val descriptorProto = DescriptorProtos.DescriptorProto.newBuilder
      .addField(0, eventId.setNumber(1))
      .addField(1, testContext(legacyColumns, 0).setNumber(2))
      .addNestedType(testNestedType100)
    fromDescriptorProtoBuilder(descriptorProto)
  }

  def withTestContext101(legacyColumns: Boolean): Descriptors.Descriptor = {
    val descriptorProto = DescriptorProtos.DescriptorProto.newBuilder
      .addField(0, eventId.setNumber(1))
      .addField(1, testContext(legacyColumns, 1).setNumber(2))
      .addNestedType(testNestedType101)
    fromDescriptorProtoBuilder(descriptorProto)
  }

  def withAdClickContext(legacyColumns: Boolean): Descriptors.Descriptor = {
    val descriptorProto = DescriptorProtos.DescriptorProto.newBuilder
      .addField(0, eventId.setNumber(1))
      .addField(1, adClickContext(legacyColumns).setNumber(2))
      .addNestedType(adClickEventNestedType)
    fromDescriptorProtoBuilder(descriptorProto)
  }

  /** A table which has been altered to add the ad_click_event unstruct event column */
  def withWebPageAndAdClick(legacyColumns: Boolean): Descriptors.Descriptor = {
    val descriptorProto = DescriptorProtos.DescriptorProto.newBuilder
      .addField(0, eventId.setNumber(1))
      .addField(1, webPage(legacyColumns).setNumber(2))
      .addField(2, adClickEvent(legacyColumns).setNumber(3))
      .addNestedType(webPageNestedType)
      .addNestedType(adClickEventNestedType)
    fromDescriptorProtoBuilder(descriptorProto)
  }

  private def fromDescriptorProtoBuilder(descriptorProto: DescriptorProtos.DescriptorProto.Builder): Descriptors.Descriptor = {
    descriptorProto.setName("event")

    val fdp = DescriptorProtos.FileDescriptorProto.newBuilder
      .addMessageType(descriptorProto)
      .build

    val fd = Descriptors.FileDescriptor.buildFrom(fdp, Array())
    fd.findMessageTypeByName("event")
  }

  private def eventId: DescriptorProtos.FieldDescriptorProto.Builder =
    DescriptorProtos.FieldDescriptorProto.newBuilder
      .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
      .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)
      .setName("event_id")

  private def webPage(legacyColumns: Boolean): DescriptorProtos.FieldDescriptorProto.Builder =
    DescriptorProtos.FieldDescriptorProto.newBuilder
      .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
      .setTypeName("web_page_1")
      .setName {
        if (legacyColumns)
          "unstruct_event_com_snowplowanalytics_snowplow_web_page_1_0_0"
        else
          "unstruct_event_com_snowplowanalytics_snowplow_web_page_1"
      }

  private def testContext(legacyColumns: Boolean, minorVersion: Int): DescriptorProtos.FieldDescriptorProto.Builder =
    DescriptorProtos.FieldDescriptorProto.newBuilder
      .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED)
      .setTypeName(s"test_10$minorVersion")
      .setName {
        if (legacyColumns)
          s"contexts_test_vendor_test_name_1_0_$minorVersion"
        else
          "contexts_test_vendor_test_name_1"
      }

  private def testUnstruct(legacyColumns: Boolean, minorVersion: Int): DescriptorProtos.FieldDescriptorProto.Builder =
    DescriptorProtos.FieldDescriptorProto.newBuilder
      .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
      .setTypeName(s"test_10$minorVersion")
      .setName {
        if (legacyColumns)
          s"unstruct_event_test_vendor_test_name_1_0_$minorVersion"
        else
          "unstruct_event_test_vendor_test_name_1"
      }

  private def webPageId: DescriptorProtos.FieldDescriptorProto.Builder =
    DescriptorProtos.FieldDescriptorProto.newBuilder
      .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
      .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)
      .setName("id")

  private def myString: DescriptorProtos.FieldDescriptorProto.Builder =
    DescriptorProtos.FieldDescriptorProto.newBuilder
      .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
      .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)
      .setName("my_string")

  private def myInteger: DescriptorProtos.FieldDescriptorProto.Builder =
    DescriptorProtos.FieldDescriptorProto.newBuilder
      .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
      .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64)
      .setName("my_integer")

  private def webPageNestedType: DescriptorProtos.DescriptorProto.Builder =
    DescriptorProtos.DescriptorProto.newBuilder
      .addField(0, webPageId.setNumber(1))
      .setName("web_page_1")

  private def testNestedType100: DescriptorProtos.DescriptorProto.Builder =
    DescriptorProtos.DescriptorProto.newBuilder
      .addField(0, myString.setNumber(1))
      .setName("test_100")

  private def testNestedType101: DescriptorProtos.DescriptorProto.Builder =
    DescriptorProtos.DescriptorProto.newBuilder
      .addField(0, myString.setNumber(1))
      .addField(1, myInteger.setNumber(2))
      .setName("test_101")

  private def adClickEvent(legacyColumns: Boolean): DescriptorProtos.FieldDescriptorProto.Builder =
    DescriptorProtos.FieldDescriptorProto.newBuilder
      .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
      .setTypeName("ad_click_event_1")
      .setName {
        if (legacyColumns)
          "unstruct_event_com_snowplowanalytics_snowplow_media_ad_click_event_1_0_0"
        else
          "unstruct_event_com_snowplowanalytics_snowplow_media_ad_click_event_1"
      }

  private def adClickContext(legacyColumns: Boolean): DescriptorProtos.FieldDescriptorProto.Builder =
    DescriptorProtos.FieldDescriptorProto.newBuilder
      .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED)
      .setTypeName("ad_click_event_1")
      .setName {
        if (legacyColumns)
          "contexts_com_snowplowanalytics_snowplow_media_ad_click_event_1_0_0"
        else
          "contexts_com_snowplowanalytics_snowplow_media_ad_click_event_1"
      }

  private def adClickEventPercentProgress: DescriptorProtos.FieldDescriptorProto.Builder =
    DescriptorProtos.FieldDescriptorProto.newBuilder
      .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
      .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT64)
      .setName("percent_progress")

  private def adClickEventNestedType: DescriptorProtos.DescriptorProto.Builder =
    DescriptorProtos.DescriptorProto.newBuilder
      .addField(0, adClickEventPercentProgress.setNumber(1))
      .setName("ad_click_event_1")

}
