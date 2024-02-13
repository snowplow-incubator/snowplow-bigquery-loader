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

  def get: Descriptors.Descriptor = {
    val descriptorProto = DescriptorProtos.DescriptorProto.newBuilder
      .addField(0, eventId.setNumber(1)) // For laziness, only adding one atomic field
    fromDescriptorProtoBuilder(descriptorProto)
  }

  def withWebPage: Descriptors.Descriptor = {
    val descriptorProto = DescriptorProtos.DescriptorProto.newBuilder
      .addField(0, eventId.setNumber(1))
      .addField(1, webPage.setNumber(2))
      .addNestedType(webPageNestedType)
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

  private def webPageId: DescriptorProtos.FieldDescriptorProto.Builder =
    DescriptorProtos.FieldDescriptorProto.newBuilder
      .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
      .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)
      .setName("id")

  private def webPageNestedType: DescriptorProtos.DescriptorProto.Builder =
    DescriptorProtos.DescriptorProto.newBuilder
      .addField(0, webPageId.setNumber(1))
      .setName("web_page_1")

}
