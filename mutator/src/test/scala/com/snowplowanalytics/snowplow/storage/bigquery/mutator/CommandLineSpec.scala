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
package com.snowplowanalytics.snowplow.storage.bigquery.mutator

import java.util.UUID

import cats.syntax.either._

import CommandLine._

import com.snowplowanalytics.snowplow.storage.bigquery.common.Config
import com.snowplowanalytics.snowplow.storage.bigquery.common.Config.EnvironmentConfig

class CommandLineSpec extends org.specs2.Specification { def is = s2"""
  parse extracts valid configuration for listen subcommand $e1
  parse extracts valid configuration for create subcommand $e2
  getEnv validates end extracts configuration $e3
  """


  def e1 = {
    val expected = ListenCommand(EnvironmentConfig(SpecHelpers.jsonResolver, SpecHelpers.jsonConfig), false)
    val result = CommandLine.parse(Seq("listen", "--resolver", SpecHelpers.base64Resolver, "--config", SpecHelpers.base64Config))
    result must beRight(expected)
  }


  def e2 = {
    val expected = CreateCommand(EnvironmentConfig(SpecHelpers.jsonResolver, SpecHelpers.jsonConfig))
    val result = CommandLine.parse(Seq("create", "--resolver", SpecHelpers.base64Resolver, "--config", SpecHelpers.base64Config))
    result must beRight(expected)
  }

  def e3 = {
    val expected = Config(
      "Snowplow BigQuery",
      UUID.fromString("ff5176f8-c0e3-4ef0-a94f-3b4f86e042ca"),
      "enriched-topic",
      "snowplow-data",
      "atomic",
      "events",
      Config.LoadMode.StreamingInserts(false),
      "types-topic",
      "types-sub",
      "bad-rows-topic",
      "failed-inserts-topic")

    val result = CommandLine
      .parse(Seq("create", "--resolver", SpecHelpers.base64Resolver, "--config", SpecHelpers.base64Config))
//      .map(_.getEnv.unsafeRunSync().config)
    result must beRight(expected)

    skipped("Config schema is not on Iglu Central yet")
  }

  def e4 = {
    val schema101 =
      """
        |{
        |    "self": {
        |      "vendor": "tech.hereford",
        |      "name": "bidresponses_context",
        |      "format": "jsonschema",
        |      "version": "1-0-1"
        |    },
        |    "$schema": "http://iglucentral.com/schemas/com.snowplowanalytics.self-desc/schema/jsonschema/1-0-0#",
        |    "description": "Schema for Hereford Tech's Ozone Project Bid Responses",
        |    "type": "object",
        |    "properties": {
        |      "timestamp": {
        |        "type": "integer",
        |        "maximum": 2147483647,
        |        "minimum": 0
        |      },
        |      "userid_ctxt": {
        |        "type": [
        |          "string",
        |          "null"
        |        ]
        |      },
        |      "page_name": {
        |        "type": "string"
        |      },
        |      "id": {
        |        "type": "string",
        |        "format": "uuid"
        |      },
        |      "seatbid": {
        |        "type": "array",
        |        "items": {
        |          "type": "object",
        |          "properties": {
        |            "bid": {
        |              "type": "array",
        |              "items": {
        |                "type": "object",
        |                "properties": {
        |                  "price": {
        |                    "type": "number",
        |                    "minimum": 0
        |                  },
        |                  "adomain": {
        |                    "type": "array",
        |                    "items": {
        |                      "type": "string"
        |                    }
        |                  },
        |                  "adm": {
        |                    "type": "string"
        |                  },
        |                  "id": {
        |                    "type": "string"
        |                  },
        |                  "ext": {
        |                    "type": "object",
        |                    "properties": {
        |                      "prebid": {
        |                        "type": "object",
        |                        "properties": {
        |                          "type": {
        |                            "type": "string"
        |                          }
        |                        },
        |                        "additionalProperties": true
        |                      }
        |                    },
        |                    "additionalProperties": true
        |                  },
        |                  "h": {
        |                    "type": "integer"
        |                  },
        |                  "w": {
        |                    "type": "integer"
        |                  },
        |                  "impid": {
        |                    "type": "string"
        |                  },
        |                  "crid": {
        |                    "type": "string"
        |                  }
        |                },
        |                "additionalProperties": true
        |              }
        |            },
        |            "seat": {
        |              "type": "string"
        |            }
        |          },
        |          "additionalProperties": true
        |        }
        |      },
        |      "ext": {
        |        "type": "object",
        |        "properties": {
        |          "debug": {
        |            "type": "object",
        |            "properties": {
        |              "httpcalls": {
        |                "type": "object",
        |                "properties": {
        |                  "appnexus": {
        |                    "type": "array",
        |                    "items": {
        |                      "type": "object",
        |                      "properties": {
        |                        "uri": {
        |                          "type": "string",
        |                          "format": "uri",
        |                          "minLength": 28,
        |                          "maxLength": 8192
        |                        },
        |                        "requestbody": {
        |                          "type": "string",
        |                          "minLength": 1
        |                        },
        |                        "responsebody": {
        |                          "type": "string"
        |                        },
        |                        "status": {
        |                          "type": "integer",
        |                          "maximum": 32767,
        |                          "minimum": 0
        |                        }
        |                      },
        |                      "additionalProperties": true
        |                    }
        |                  },
        |                  "openx": {
        |                    "type": "array",
        |                    "items": {
        |                      "type": "object",
        |                      "properties": {
        |                        "uri": {
        |                          "type": "string",
        |                          "format": "uri",
        |                          "minLength": 27,
        |                          "maxLength": 8192
        |                        },
        |                        "requestbody": {
        |                          "type": "string"
        |                        },
        |                        "responsebody": {
        |                          "type": "string"
        |                        },
        |                        "status": {
        |                          "type": "integer"
        |                        }
        |                      },
        |                      "additionalProperties": true
        |                    }
        |                  }
        |                },
        |                "additionalProperties": true
        |              },
        |              "resolvedrequest": {
        |                "type": "object",
        |                "properties": {
        |                  "test": {
        |                    "type": "integer",
        |                    "maximum": 32767,
        |                    "minimum": 0
        |                  },
        |                  "source": {
        |                    "type": "object",
        |                    "properties": {
        |                      "tid": {
        |                        "type": "string",
        |                        "format": "uuid"
        |                      }
        |                    },
        |                    "additionalProperties": true
        |                  },
        |                  "tmax": {
        |                    "type": "integer",
        |                    "maximum": 32767,
        |                    "minimum": 0
        |                  },
        |                  "site": {
        |                    "type": "object",
        |                    "properties": {
        |                      "domain": {
        |                        "type": "string"
        |                      },
        |                      "page": {
        |                        "type": "string",
        |                        "format": "uri",
        |                        "minLength": 32,
        |                        "maxLength": 8192
        |                      },
        |                      "publisher": {
        |                        "type": "object",
        |                        "properties": {
        |                          "id": {
        |                            "type": "string"
        |                          }
        |                        },
        |                        "additionalProperties": true
        |                      }
        |                    },
        |                    "additionalProperties": true
        |                  },
        |                  "at": {
        |                    "type": "integer",
        |                    "maximum": 32767,
        |                    "minimum": 0
        |                  },
        |                  "id": {
        |                    "type": "string",
        |                    "format": "uuid"
        |                  },
        |                  "ext": {
        |                    "type": "object",
        |                    "properties": {
        |                      "City": {
        |                        "type": "string"
        |                      },
        |                      "Country": {
        |                        "type": "string"
        |                      }
        |                    },
        |                    "additionalProperties": true
        |                  },
        |                  "imp": {
        |                    "type": "array",
        |                    "items": {
        |                      "type": "object",
        |                      "properties": {
        |                        "id": {
        |                          "type": "string"
        |                        },
        |                        "banner": {
        |                          "type": "object",
        |                          "properties": {
        |                            "format": {
        |                              "type": "array",
        |                              "items": {
        |                                "type": "object",
        |                                "properties": {
        |                                  "w": {
        |                                    "type": "integer",
        |                                    "maximum": 32767,
        |                                    "minimum": 0
        |                                  },
        |                                  "h": {
        |                                    "type": "integer",
        |                                    "maximum": 32767,
        |                                    "minimum": 0
        |                                  }
        |                                },
        |                                "additionalProperties": true
        |                              }
        |                            }
        |                          },
        |                          "additionalProperties": true
        |                        },
        |                        "ext": {
        |                          "type": "object",
        |                          "properties": {
        |                            "openx": {
        |                              "type": "object",
        |                              "properties": {
        |                                "unit": {
        |                                  "type": "string"
        |                                },
        |                                "delDomain": {
        |                                  "type": "string"
        |                                },
        |                                "lotame": {
        |                                  "type": "string"
        |                                },
        |                                "customData": {
        |                                  "type": "string"
        |                                }
        |                              },
        |                              "additionalProperties": true
        |                            },
        |                            "appnexus": {
        |                              "type": "object",
        |                              "properties": {
        |                                "placementId": {
        |                                  "type": "integer",
        |                                  "maximum": 2147483647,
        |                                  "minimum": 0
        |                                },
        |                                "lotame": {
        |                                  "type": "string"
        |                                },
        |                                "customData": {
        |                                  "type": "string"
        |                                }
        |                              },
        |                              "additionalProperties": true
        |                            }
        |                          },
        |                          "additionalProperties": true
        |                        }
        |                      },
        |                      "additionalProperties": true
        |                    }
        |                  },
        |                  "user": {
        |                    "type": "object",
        |                    "properties": {
        |                      "id": {
        |                        "type": "string"
        |                      }
        |                    },
        |                    "additionalProperties": true
        |                  },
        |                  "device": {
        |                    "type": "object",
        |                    "properties": {
        |                      "ua": {
        |                        "type": "string"
        |                      },
        |                      "ip": {
        |                        "type": "string",
        |                        "format": "ipv4"
        |                      }
        |                    },
        |                    "additionalProperties": true
        |                  }
        |                },
        |                "additionalProperties": true
        |              }
        |            },
        |            "additionalProperties": true
        |          },
        |          "responsetimemillis": {
        |            "type": "object",
        |            "properties": {
        |              "appnexus": {
        |                "type": "integer",
        |                "maximum": 32767,
        |                "minimum": 0
        |              },
        |              "openx": {
        |                "type": "integer",
        |                "maximum": 32767,
        |                "minimum": 0
        |              }
        |            },
        |            "additionalProperties": true
        |          }
        |        },
        |        "additionalProperties": true
        |      }
        |    },
        |    "additionalProperties": true
        |  }
      """.stripMargin

    val schema100 =
      """
        |{
        |    "self": {
        |      "vendor": "tech.hereford",
        |      "name": "bidresponses_context",
        |      "format": "jsonschema",
        |      "version": "1-0-1"
        |    },
        |    "$schema": "http://iglucentral.com/schemas/com.snowplowanalytics.self-desc/schema/jsonschema/1-0-0#",
        |    "description": "Schema for Hereford Tech's Ozone Project Bid Responses",
        |    "type": "object",
        |    "properties": {
        |      "timestamp": {
        |        "type": "integer",
        |        "maximum": 2147483647,
        |        "minimum": 0
        |      },
        |      "userid_ctxt": {
        |        "type": [
        |          "string",
        |          "null"
        |        ]
        |      },
        |      "page_name": {
        |        "type": "string"
        |      },
        |      "id": {
        |        "type": "string",
        |        "format": "uuid"
        |      },
        |      "seatbid": {
        |        "type": "array",
        |        "items": {
        |          "type": "object",
        |          "properties": {
        |            "bid": {
        |              "type": "array",
        |              "items": {
        |                "type": "object",
        |                "properties": {
        |                  "price": {
        |                    "type": "number",
        |                    "minimum": 0
        |                  },
        |                  "adomain": {
        |                    "type": "array",
        |                    "items": {
        |                      "type": "string"
        |                    }
        |                  },
        |                  "adm": {
        |                    "type": "string"
        |                  },
        |                  "id": {
        |                    "type": "string"
        |                  },
        |                  "ext": {
        |                    "type": "object",
        |                    "properties": {
        |                      "prebid": {
        |                        "type": "object",
        |                        "properties": {
        |                          "type": {
        |                            "type": "string"
        |                          }
        |                        },
        |                        "additionalProperties": true
        |                      }
        |                    },
        |                    "additionalProperties": true
        |                  },
        |                  "h": {
        |                    "type": "integer"
        |                  },
        |                  "w": {
        |                    "type": "integer"
        |                  },
        |                  "impid": {
        |                    "type": "string"
        |                  },
        |                  "crid": {
        |                    "type": "string"
        |                  }
        |                },
        |                "additionalProperties": true
        |              }
        |            },
        |            "seat": {
        |              "type": "string"
        |            }
        |          },
        |          "additionalProperties": true
        |        }
        |      },
        |      "ext": {
        |        "type": "object",
        |        "properties": {
        |          "debug": {
        |            "type": "object",
        |            "properties": {
        |              "httpcalls": {
        |                "type": "object",
        |                "properties": {
        |                  "appnexus": {
        |                    "type": "array",
        |                    "items": {
        |                      "type": "object",
        |                      "properties": {
        |                        "uri": {
        |                          "type": "string",
        |                          "format": "uri",
        |                          "minLength": 28,
        |                          "maxLength": 8192
        |                        },
        |                        "requestbody": {
        |                          "type": "string",
        |                          "minLength": 1
        |                        },
        |                        "responsebody": {
        |                          "type": "string"
        |                        },
        |                        "status": {
        |                          "type": "integer",
        |                          "maximum": 32767,
        |                          "minimum": 0
        |                        }
        |                      },
        |                      "additionalProperties": true
        |                    }
        |                  },
        |                  "openx": {
        |                    "type": "array",
        |                    "items": {
        |                      "type": "object",
        |                      "properties": {
        |                        "uri": {
        |                          "type": "string",
        |                          "format": "uri",
        |                          "minLength": 27,
        |                          "maxLength": 8192
        |                        },
        |                        "requestbody": {
        |                          "type": "string"
        |                        },
        |                        "responsebody": {
        |                          "type": "string"
        |                        },
        |                        "status": {
        |                          "type": "integer"
        |                        }
        |                      },
        |                      "additionalProperties": true
        |                    }
        |                  }
        |                },
        |                "additionalProperties": true
        |              },
        |              "resolvedrequest": {
        |                "type": "object",
        |                "properties": {
        |                  "test": {
        |                    "type": "integer",
        |                    "maximum": 32767,
        |                    "minimum": 0
        |                  },
        |                  "source": {
        |                    "type": "object",
        |                    "properties": {
        |                      "tid": {
        |                        "type": "string",
        |                        "format": "uuid"
        |                      }
        |                    },
        |                    "additionalProperties": true
        |                  },
        |                  "tmax": {
        |                    "type": "integer",
        |                    "maximum": 32767,
        |                    "minimum": 0
        |                  },
        |                  "site": {
        |                    "type": "object",
        |                    "properties": {
        |                      "domain": {
        |                        "type": "string"
        |                      },
        |                      "page": {
        |                        "type": "string",
        |                        "format": "uri",
        |                        "minLength": 32,
        |                        "maxLength": 8192
        |                      },
        |                      "publisher": {
        |                        "type": "object",
        |                        "properties": {
        |                          "id": {
        |                            "type": "string"
        |                          }
        |                        },
        |                        "additionalProperties": true
        |                      }
        |                    },
        |                    "additionalProperties": true
        |                  },
        |                  "at": {
        |                    "type": "integer",
        |                    "maximum": 32767,
        |                    "minimum": 0
        |                  },
        |                  "id": {
        |                    "type": "string",
        |                    "format": "uuid"
        |                  },
        |                  "ext": {
        |                    "type": "object",
        |                    "properties": {
        |                      "City": {
        |                        "type": "string"
        |                      },
        |                      "Country": {
        |                        "type": "string"
        |                      }
        |                    },
        |                    "additionalProperties": true
        |                  },
        |                  "imp": {
        |                    "type": "array",
        |                    "items": {
        |                      "type": "object",
        |                      "properties": {
        |                        "id": {
        |                          "type": "string"
        |                        },
        |                        "banner": {
        |                          "type": "object",
        |                          "properties": {
        |                            "format": {
        |                              "type": "array",
        |                              "items": {
        |                                "type": "object",
        |                                "properties": {
        |                                  "w": {
        |                                    "type": "integer",
        |                                    "maximum": 32767,
        |                                    "minimum": 0
        |                                  },
        |                                  "h": {
        |                                    "type": "integer",
        |                                    "maximum": 32767,
        |                                    "minimum": 0
        |                                  }
        |                                },
        |                                "additionalProperties": true
        |                              }
        |                            }
        |                          },
        |                          "additionalProperties": true
        |                        },
        |                        "ext": {
        |                          "type": "object",
        |                          "properties": {
        |                            "openx": {
        |                              "type": "object",
        |                              "properties": {
        |                                "unit": {
        |                                  "type": "string"
        |                                },
        |                                "delDomain": {
        |                                  "type": "string"
        |                                },
        |                                "lotame": {
        |                                  "type": "string"
        |                                },
        |                                "customData": {
        |                                  "type": "string"
        |                                }
        |                              },
        |                              "additionalProperties": true
        |                            },
        |                            "appnexus": {
        |                              "type": "object",
        |                              "properties": {
        |                                "placementId": {
        |                                  "type": "integer",
        |                                  "maximum": 2147483647,
        |                                  "minimum": 0
        |                                },
        |                                "lotame": {
        |                                  "type": "string"
        |                                },
        |                                "customData": {
        |                                  "type": "string"
        |                                }
        |                              },
        |                              "additionalProperties": true
        |                            }
        |                          },
        |                          "additionalProperties": true
        |                        }
        |                      },
        |                      "additionalProperties": true
        |                    }
        |                  },
        |                  "user": {
        |                    "type": "object",
        |                    "properties": {
        |                      "id": {
        |                        "type": "string"
        |                      }
        |                    },
        |                    "additionalProperties": true
        |                  },
        |                  "device": {
        |                    "type": "object",
        |                    "properties": {
        |                      "ua": {
        |                        "type": "string"
        |                      },
        |                      "ip": {
        |                        "type": "string",
        |                        "format": "ipv4"
        |                      }
        |                    },
        |                    "additionalProperties": true
        |                  }
        |                },
        |                "additionalProperties": true
        |              }
        |            },
        |            "additionalProperties": true
        |          },
        |          "responsetimemillis": {
        |            "type": "object",
        |            "properties": {
        |              "appnexus": {
        |                "type": "integer",
        |                "maximum": 32767,
        |                "minimum": 0
        |              },
        |              "openx": {
        |                "type": "integer",
        |                "maximum": 32767,
        |                "minimum": 0
        |              }
        |            },
        |            "additionalProperties": true
        |          }
        |        },
        |        "additionalProperties": true
        |      }
        |    },
        |    "additionalProperties": true
        |  }
      """.stripMargin

    import com.snowplowanalytics.iglu.schemaddl.bigquery.{Mode, Field => DdlField}

    val schema0 = SpecHelpers.parseSchema(schema100)
    val schema1 = SpecHelpers.parseSchema(schema101)

    val r0 = DdlField.build("foo0", schema, false)
    val r1 = DdlField.build("foo1", schema, false)

    println(r0)
    println(r1)

    ok
  }
}