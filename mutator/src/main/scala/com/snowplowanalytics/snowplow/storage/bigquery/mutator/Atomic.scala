package com.snowplowanalytics.snowplow.storage.bigquery.mutator

import com.snowplowanalytics.iglu.schemaddl.bigquery.Field
import com.snowplowanalytics.iglu.schemaddl.bigquery.{ Type => DataType }
import com.snowplowanalytics.iglu.schemaddl.bigquery.Mode

object Atomic {

  val table = List(
    // App
    Field("app_id", DataType.String, Mode.Nullable),
    Field("platform", DataType.String, Mode.Nullable),

    // Data/time
    Field("etl_tstamp", DataType.Timestamp, Mode.Nullable),
    Field("collector_tstamp", DataType.Timestamp, Mode.Required),
    Field("dvce_created_tstamp", DataType.Timestamp, Mode.Nullable),

    // Event
    Field("event", DataType.String, Mode.Nullable),
    Field("event_id", DataType.String, Mode.Required),
    Field("txn_id", DataType.Integer, Mode.Nullable),

    // Namespacing and versioning
    Field("name_tracker", DataType.String, Mode.Nullable),
    Field("v_tracker", DataType.String, Mode.Nullable),
    Field("v_collector", DataType.String, Mode.Required),
    Field("v_etl", DataType.String, Mode.Required),

    // User id and visit
    Field("user_id", DataType.String, Mode.Nullable),
    Field("user_ipaddress", DataType.String, Mode.Nullable),
    Field("user_fingerprint", DataType.String, Mode.Nullable),
    Field("domain_userid", DataType.String, Mode.Nullable),
    Field("domain_sessionidx", DataType.Integer, Mode.Nullable),
    Field("network_userid", DataType.String, Mode.Nullable),

    // Location
    Field("geo_country", DataType.String, Mode.Nullable),
    Field("geo_region", DataType.String, Mode.Nullable),
    Field("geo_city", DataType.String, Mode.Nullable),
    Field("geo_zipcode", DataType.String, Mode.Nullable),
    Field("geo_latitude", DataType.Float, Mode.Nullable),
    Field("geo_longitude", DataType.Float, Mode.Nullable),
    Field("geo_region_name", DataType.String, Mode.Nullable),

    // Ip lookups
    Field("ip_isp", DataType.String, Mode.Nullable),
    Field("ip_organization", DataType.String, Mode.Nullable),
    Field("ip_domain", DataType.String, Mode.Nullable),
    Field("ip_netspeed", DataType.String, Mode.Nullable),

    // Page
    Field("page_url", DataType.String, Mode.Nullable),
    Field("page_title", DataType.String, Mode.Nullable),
    Field("page_referrer", DataType.String, Mode.Nullable),

    // Page URL components
    Field("page_urlscheme", DataType.String, Mode.Nullable),
    Field("page_urlhost", DataType.String, Mode.Nullable),
    Field("page_urlport", DataType.Integer, Mode.Nullable),
    Field("page_urlpath", DataType.String, Mode.Nullable),
    Field("page_urlquery", DataType.String, Mode.Nullable),
    Field("page_urlfragment", DataType.String, Mode.Nullable),

    // Referrer URL components
    Field("refr_urlscheme", DataType.String, Mode.Nullable),
    Field("refr_urlhost", DataType.String, Mode.Nullable),
    Field("refr_urlport", DataType.Integer, Mode.Nullable),
    Field("refr_urlpath", DataType.String, Mode.Nullable),
    Field("refr_urlquery", DataType.String, Mode.Nullable),
    Field("refr_urlfragment", DataType.String, Mode.Nullable),

    // Referrer details
    Field("refr_medium", DataType.String, Mode.Nullable),
    Field("refr_source", DataType.String, Mode.Nullable),
    Field("refr_term", DataType.String, Mode.Nullable),

    // Marketing
    Field("mkt_medium", DataType.String, Mode.Nullable),
    Field("mkt_source", DataType.String, Mode.Nullable),
    Field("mkt_term", DataType.String, Mode.Nullable),
    Field("mkt_content", DataType.String, Mode.Nullable),
    Field("mkt_campaign", DataType.String, Mode.Nullable),

    // Custom structured event
    Field("se_category", DataType.String, Mode.Nullable),
    Field("se_action", DataType.String, Mode.Nullable),
    Field("se_label", DataType.String, Mode.Nullable),
    Field("se_property", DataType.String, Mode.Nullable),
    Field("se_value", DataType.Float, Mode.Nullable),

    // Ecommerce
    Field("tr_orderid", DataType.String, Mode.Nullable),
    Field("tr_affiliation", DataType.String, Mode.Nullable),
    Field("tr_total", DataType.Float, Mode.Nullable),
    Field("tr_tax", DataType.Float, Mode.Nullable),
    Field("tr_shipping", DataType.Float, Mode.Nullable),
    Field("tr_city", DataType.String, Mode.Nullable),
    Field("tr_state", DataType.String, Mode.Nullable),
    Field("tr_country", DataType.String, Mode.Nullable),
    Field("ti_orderid", DataType.String, Mode.Nullable),
    Field("ti_sku", DataType.String, Mode.Nullable),
    Field("ti_name", DataType.String, Mode.Nullable),
    Field("ti_category", DataType.String, Mode.Nullable),
    Field("ti_price", DataType.Float, Mode.Nullable),
    Field("ti_quantity", DataType.Integer, Mode.Nullable),

    // Page ping
    Field("pp_xoffset_min", DataType.Integer, Mode.Nullable),
    Field("pp_xoffset_max", DataType.Integer, Mode.Nullable),
    Field("pp_yoffset_min", DataType.Integer, Mode.Nullable),
    Field("pp_yoffset_max", DataType.Integer, Mode.Nullable),

    // Useragent
    Field("useragent", DataType.String, Mode.Nullable),

    // Browser
    Field("br_name", DataType.String, Mode.Nullable),
    Field("br_family", DataType.String, Mode.Nullable),
    Field("br_version", DataType.String, Mode.Nullable),
    Field("br_type", DataType.String, Mode.Nullable),
    Field("br_renderengine", DataType.String, Mode.Nullable),
    Field("br_lang", DataType.String, Mode.Nullable),
    Field("br_features_pdf", DataType.Boolean, Mode.Nullable),
    Field("br_features_flash", DataType.Boolean, Mode.Nullable),
    Field("br_features_java", DataType.Boolean, Mode.Nullable),
    Field("br_features_director", DataType.Boolean, Mode.Nullable),
    Field("br_features_quicktime", DataType.Boolean, Mode.Nullable),
    Field("br_features_realplayer", DataType.Boolean, Mode.Nullable),
    Field("br_features_windowsmedia", DataType.Boolean, Mode.Nullable),
    Field("br_features_gears", DataType.Boolean, Mode.Nullable),
    Field("br_features_silverlight", DataType.Boolean, Mode.Nullable),
    Field("br_cookies", DataType.Boolean, Mode.Nullable),
    Field("br_colordepth", DataType.String, Mode.Nullable),
    Field("br_viewwidth", DataType.Integer, Mode.Nullable),
    Field("br_viewheight", DataType.Integer, Mode.Nullable),

    // Operating System
    Field("os_name", DataType.String, Mode.Nullable),
    Field("os_family", DataType.String, Mode.Nullable),
    Field("os_manufacturer", DataType.String, Mode.Nullable),
    Field("os_timezone", DataType.String, Mode.Nullable),

    // Device/Hardware
    Field("dvce_type", DataType.String, Mode.Nullable),
    Field("dvce_ismobile", DataType.Boolean, Mode.Nullable),
    Field("dvce_screenwidth", DataType.Integer, Mode.Nullable),
    Field("dvce_screenheight", DataType.Integer, Mode.Nullable),

    // Document
    Field("doc_charset", DataType.String, Mode.Nullable),
    Field("doc_width", DataType.Integer, Mode.Nullable),
    Field("doc_height", DataType.Integer, Mode.Nullable),

    // Currency
    Field("tr_currency", DataType.String, Mode.Nullable),
    Field("tr_total_base", DataType.Float, Mode.Nullable),
    Field("tr_tax_base", DataType.Float, Mode.Nullable),
    Field("tr_shipping_base", DataType.Float, Mode.Nullable),
    Field("ti_currency", DataType.String, Mode.Nullable),
    Field("ti_price_base", DataType.Float, Mode.Nullable),
    Field("base_currency", DataType.String, Mode.Nullable),

    // Geolocation
    Field("geo_timezone", DataType.String, Mode.Nullable),

    // Click ID
    Field("mkt_clickid", DataType.String, Mode.Nullable),
    Field("mkt_network", DataType.String, Mode.Nullable),

    // ETL Tags
    Field("etl_tags", DataType.String, Mode.Nullable),

    // Time event was sent
    Field("dvce_sent_tstamp", DataType.Timestamp, Mode.Nullable),

    // Referer
    Field("refr_domain_userid", DataType.String, Mode.Nullable),
    Field("refr_dvce_tstamp", DataType.Timestamp, Mode.Nullable),

    // Session ID
    Field("domain_sessionid", DataType.String, Mode.Nullable),

    // Derived timestamp
    Field("derived_tstamp", DataType.Timestamp, Mode.Nullable),

    // Event schema
    Field("event_vendor", DataType.String, Mode.Nullable),
    Field("event_name", DataType.String, Mode.Nullable),
    Field("event_format", DataType.String, Mode.Nullable),
    Field("event_version", DataType.String, Mode.Nullable),

    // Event fingerprint
    Field("event_fingerprint", DataType.String, Mode.Nullable),

    // True timestamp
    Field("true_tstamp", DataType.Timestamp, Mode.Nullable)
  )
}
