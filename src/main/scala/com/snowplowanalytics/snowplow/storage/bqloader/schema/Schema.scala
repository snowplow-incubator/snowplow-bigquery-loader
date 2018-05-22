/*
 * Copyright (c) 2018 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow.storage.bqloader.schema

import scala.collection.convert.decorateAsJava._

import com.google.api.services.bigquery.model.{TableFieldSchema, TableSchema}

object Schema {
  val atomicColumns = List(
    // App
    Column("app_id", DataType.String),
    Column("platform", DataType.String),

    // Data/time
    Column("etl_tstamp", DataType.Timestamp),
    Column("collector_tstamp", DataType.Timestamp, Mode.Required),
    Column("dvce_created_tstamp", DataType.Timestamp),

    // Event
    Column("event", DataType.String),
    Column("event_id", DataType.String, Mode.Required),
    Column("txn_id", DataType.Integer),

    // Namespacing and versioning
    Column("name_tracker", DataType.String),
    Column("v_tracker", DataType.String),
    Column("v_collector", DataType.String, Mode.Required),
    Column("v_etl", DataType.String, Mode.Required),

    // User id and visit
    Column("user_id", DataType.String),
    Column("user_ipaddress", DataType.String),
    Column("user_fingerprint", DataType.String),
    Column("domain_userid", DataType.String),
    Column("domain_sessionidx", DataType.Integer),
    Column("network_userid", DataType.String),

    // Location
    Column("geo_country", DataType.String),
    Column("geo_region", DataType.String),
    Column("geo_city", DataType.String),
    Column("geo_zipcode", DataType.String),
    Column("geo_latitude", DataType.FloatingPoint),
    Column("geo_longitude", DataType.FloatingPoint),
    Column("geo_region_name", DataType.String),

    // Ip lookups
    Column("ip_isp", DataType.String),
    Column("ip_organization", DataType.String),
    Column("ip_domain", DataType.String),
    Column("ip_netspeed", DataType.String),

    // Page
    Column("page_url", DataType.String),
    Column("page_title", DataType.String),
    Column("page_referrer", DataType.String),

    // Page URL components
    Column("page_urlscheme", DataType.String),
    Column("page_urlhost", DataType.String),
    Column("page_urlport", DataType.Integer),
    Column("page_urlpath", DataType.String),
    Column("page_urlquery", DataType.String),
    Column("page_urlfragment", DataType.String),

    // Referrer URL components
    Column("refr_urlscheme", DataType.String),
    Column("refr_urlhost", DataType.String),
    Column("refr_urlport", DataType.Integer),
    Column("refr_urlpath", DataType.String),
    Column("refr_urlquery", DataType.String),
    Column("refr_urlfragment", DataType.String),

    // Referrer details
    Column("refr_medium", DataType.String),
    Column("refr_source", DataType.String),
    Column("refr_term", DataType.String),

    // Marketing
    Column("mkt_medium", DataType.String),
    Column("mkt_source", DataType.String),
    Column("mkt_term", DataType.String),
    Column("mkt_content", DataType.String),
    Column("mkt_campaign", DataType.String),

    // Custom structured event
    Column("se_category", DataType.String),
    Column("se_action", DataType.String),
    Column("se_label", DataType.String),
    Column("se_property", DataType.String),
    Column("se_value", DataType.FloatingPoint),

    // Ecommerce
    Column("tr_orderid", DataType.String),
    Column("tr_affiliation", DataType.String),
    Column("tr_total", DataType.FloatingPoint),
    Column("tr_tax", DataType.FloatingPoint),
    Column("tr_shipping", DataType.FloatingPoint),
    Column("tr_city", DataType.String),
    Column("tr_state", DataType.String),
    Column("tr_country", DataType.String),
    Column("ti_orderid", DataType.String),
    Column("ti_sku", DataType.String),
    Column("ti_name", DataType.String),
    Column("ti_category", DataType.String),
    Column("ti_price", DataType.FloatingPoint),
    Column("ti_quantity", DataType.Integer),

    // Page ping
    Column("pp_xoffset_min", DataType.Integer),
    Column("pp_xoffset_max", DataType.Integer),
    Column("pp_yoffset_min", DataType.Integer),
    Column("pp_yoffset_max", DataType.Integer),

    // Useragent
    Column("useragent", DataType.String),

    // Browser
    Column("br_name", DataType.String),
    Column("br_family", DataType.String),
    Column("br_version", DataType.String),
    Column("br_type", DataType.String),
    Column("br_renderengine", DataType.String),
    Column("br_lang", DataType.String),
    Column("br_features_pdf", DataType.Boolean),
    Column("br_features_flash", DataType.Boolean),
    Column("br_features_java", DataType.Boolean),
    Column("br_features_director", DataType.Boolean),
    Column("br_features_quicktime", DataType.Boolean),
    Column("br_features_realplayer", DataType.Boolean),
    Column("br_features_windowsmedia", DataType.Boolean),
    Column("br_features_gears", DataType.Boolean),
    Column("br_features_silverlight", DataType.Boolean),
    Column("br_cookies", DataType.Boolean),
    Column("br_colordepth", DataType.String),
    Column("br_viewwidth", DataType.Integer),
    Column("br_viewheight", DataType.Integer),

    // Operating System
    Column("os_name", DataType.String),
    Column("os_family", DataType.String),
    Column("os_manufacturer", DataType.String),
    Column("os_timezone", DataType.String),

    // Device/Hardware
    Column("dvce_type", DataType.String),
    Column("dvce_ismobile", DataType.Boolean),
    Column("dvce_screenwidth", DataType.Integer),
    Column("dvce_screenheight", DataType.Integer),

    // Document
    Column("doc_charset", DataType.String),
    Column("doc_width", DataType.Integer),
    Column("doc_height", DataType.Integer),

    // Currency
    Column("tr_currency", DataType.String),
    Column("tr_total_base", DataType.FloatingPoint),
    Column("tr_tax_base", DataType.FloatingPoint),
    Column("tr_shipping_base", DataType.FloatingPoint),
    Column("ti_currency", DataType.String),
    Column("ti_price_base", DataType.FloatingPoint),
    Column("base_currency", DataType.String),

    // Geolocation
    Column("geo_timezone", DataType.String),

    // Click ID
    Column("mkt_clickid", DataType.String),
    Column("mkt_network", DataType.String),

    // ETL Tags
    Column("etl_tags", DataType.String),

    // Time event was sent
    Column("dvce_sent_tstamp", DataType.Timestamp),

    // Referer
    Column("refr_domain_userid", DataType.String),
    Column("refr_dvce_tstamp", DataType.Timestamp),

    // Session ID
    Column("domain_sessionid", DataType.String),

    // Derived timestamp
    Column("derived_tstamp", DataType.Timestamp),

    // Event schema
    Column("event_vendor", DataType.String),
    Column("event_name", DataType.String),
    Column("event_format", DataType.String),
    Column("event_version", DataType.String),

    // Event fingerprint
    Column("event_fingerprint", DataType.String),

    // True timestamp
    Column("true_tstamp", DataType.Timestamp)
  )

  def getAtomic: TableSchema = {
    val tableSchema = new TableSchema()
    val fields = for {
      column <- atomicColumns
    } yield new TableFieldSchema()
      .setName(column.name)
      .setType(column.dataType.toDdl)
      .setMode(column.mode.toDdl)

    tableSchema.setFields(fields.asJava)
  }
}

