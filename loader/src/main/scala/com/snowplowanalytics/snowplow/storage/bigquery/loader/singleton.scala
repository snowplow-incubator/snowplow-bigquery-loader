package com.snowplowanalytics.snowplow.storage.bigquery.loader

import com.snowplowanalytics.iglu.client.Resolver
import org.json4s.JsonAST.JValue

object singleton {
  /** Singleton for Resolver to maintain one per node. */
  object ResolverSingleton {
    @volatile private var instance: Resolver = _
    /**
      * Retrieve or build an instance of a Resolver.
      */
    def get(r: JValue): Resolver = {
      if (instance == null) {
        synchronized {
          if (instance == null) {
            instance = Resolver.parse(r).fold(e => throw new RuntimeException(e.toString), identity)
          }
        }
      }
      instance
    }
  }

}
