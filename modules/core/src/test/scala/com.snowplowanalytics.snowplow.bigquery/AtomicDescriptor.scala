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
  val get: Descriptors.Descriptor = {

    val eventId = DescriptorProtos.FieldDescriptorProto.newBuilder
      .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
      .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)
      .setName("event_id")
      .setNumber(1)

    val descriptorProto = DescriptorProtos.DescriptorProto.newBuilder
      .setName("event")
      .addField(0, eventId) // For laziness, only adding one atomic field

    val fdp = DescriptorProtos.FileDescriptorProto.newBuilder
      .addMessageType(descriptorProto)
      .build

    val fd = Descriptors.FileDescriptor.buildFrom(fdp, Array())
    fd.findMessageTypeByName("event")
  }

}
