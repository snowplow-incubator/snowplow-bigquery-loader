/**
 * Copyright (c) 2013-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd., under the terms of the Snowplow
 * Limited Use License Agreement, Version 1.0 located at
 * https://docs.snowplow.io/limited-use-license-1.0 BY INSTALLING, DOWNLOADING, ACCESSING, USING OR
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
  def withWebPage: Descriptors.Descriptor = {
    val descriptorProto = DescriptorProtos.DescriptorProto.newBuilder
      .addField(0, eventId.setNumber(1))
      .addField(1, webPage.setNumber(2))
      .addNestedType(webPageNestedType)
    fromDescriptorProtoBuilder(descriptorProto)
  }

  def withTestUnstruct100: Descriptors.Descriptor = {
    val descriptorProto = DescriptorProtos.DescriptorProto.newBuilder
      .addField(0, eventId.setNumber(1))
      .addField(1, testUnstruct.setNumber(2))
      .addNestedType(testNestedType100)
    fromDescriptorProtoBuilder(descriptorProto)
  }

  def withTestUnstruct101: Descriptors.Descriptor = {
    val descriptorProto = DescriptorProtos.DescriptorProto.newBuilder
      .addField(0, eventId.setNumber(1))
      .addField(1, testUnstruct.setNumber(2))
      .addNestedType(testNestedType101)
    fromDescriptorProtoBuilder(descriptorProto)
  }

  def withTestContext100: Descriptors.Descriptor = {
    val descriptorProto = DescriptorProtos.DescriptorProto.newBuilder
      .addField(0, eventId.setNumber(1))
      .addField(1, testContext.setNumber(2))
      .addNestedType(testNestedType100)
    fromDescriptorProtoBuilder(descriptorProto)
  }

  def withTestContext101: Descriptors.Descriptor = {
    val descriptorProto = DescriptorProtos.DescriptorProto.newBuilder
      .addField(0, eventId.setNumber(1))
      .addField(1, testContext.setNumber(2))
      .addNestedType(testNestedType101)
    fromDescriptorProtoBuilder(descriptorProto)
  }

  def withAdClickContext: Descriptors.Descriptor = {
    val descriptorProto = DescriptorProtos.DescriptorProto.newBuilder
      .addField(0, eventId.setNumber(1))
      .addField(1, adClickContext.setNumber(2))
      .addNestedType(adClickEventNestedType)
    fromDescriptorProtoBuilder(descriptorProto)
  }

  /** A table which has been altered to add the ad_click_event unstruct event column */
  def withWebPageAndAdClick: Descriptors.Descriptor = {
    val descriptorProto = DescriptorProtos.DescriptorProto.newBuilder
      .addField(0, eventId.setNumber(1))
      .addField(1, webPage.setNumber(2))
      .addField(2, adClickEvent.setNumber(3))
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

  private def webPage: DescriptorProtos.FieldDescriptorProto.Builder = DescriptorProtos.FieldDescriptorProto.newBuilder
    .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
    .setTypeName("web_page_1")
    .setName("unstruct_event_com_snowplowanalytics_snowplow_web_page_1")

  private def testContext: DescriptorProtos.FieldDescriptorProto.Builder = DescriptorProtos.FieldDescriptorProto.newBuilder
    .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED)
    .setTypeName("test_1")
    .setName("contexts_test_vendor_test_name_1")

  private def testUnstruct: DescriptorProtos.FieldDescriptorProto.Builder = DescriptorProtos.FieldDescriptorProto.newBuilder
    .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
    .setTypeName("test_1")
    .setName("unstruct_event_test_vendor_test_name_1")

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
      .setName("test_1")

  private def testNestedType101: DescriptorProtos.DescriptorProto.Builder =
    DescriptorProtos.DescriptorProto.newBuilder
      .addField(0, myString.setNumber(1))
      .addField(1, myInteger.setNumber(2))
      .setName("test_1")

  private def adClickEvent: DescriptorProtos.FieldDescriptorProto.Builder = DescriptorProtos.FieldDescriptorProto.newBuilder
    .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
    .setTypeName("ad_click_event_1")
    .setName("unstruct_event_com_snowplowanalytics_snowplow_media_ad_click_event_1")

  private def adClickContext: DescriptorProtos.FieldDescriptorProto.Builder = DescriptorProtos.FieldDescriptorProto.newBuilder
    .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED)
    .setTypeName("ad_click_event_1")
    .setName("contexts_com_snowplowanalytics_snowplow_media_ad_click_event_1")

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
