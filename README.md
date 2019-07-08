# Snowplow BigQuery Loader

[![Build Status][travis-image]][travis]
[![Release][release-image]][releases]
[![License][license-image]][license]

This project contains applications used to load [Snowplow][snowplow] enriched data into [Google BigQuery][bigquery].

## Quickstart

Assuming git and [SBT][sbt] installed:

```bash
$ git clone https://github.com/snowplow-incubator/snowplow-bigquery-loader
$ cd snowplow-bigquery-loader
$ sbt "project loader" test
$ sbt "project mutator" test
```

## Find out more

| **[Technical Docs][techdocs]**    | **[Setup Guide][setup]**    | **[Contributing][contributing]**          |
|-----------------------------------|-----------------------------|-------------------------------------------|
| [![i1][techdocs-image]][techdocs] | [![i2][setup-image]][setup] | [![i3][contributing-image]][contributing] |


## Copyright and License

Snowplow BigQuery Loader is copyright 2018 Snowplow Analytics Ltd.

Licensed under the **[Apache License, Version 2.0][license]** (the "License");
you may not use this software except in compliance with the License.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

[snowplow]: https://github.com/snowplow/snowplow/
[bigquery]: https://cloud.google.com/bigquery/
[sbt]: https://www.scala-sbt.org/

[license-image]: http://img.shields.io/badge/license-Apache--2-blue.svg?style=flat
[license]: http://www.apache.org/licenses/LICENSE-2.0

[travis]: https://travis-ci.org/snowplow-incubator/snowplow-bigquery-loader
[travis-image]: https://travis-ci.org/snowplow-incubator/snowplow-bigquery-loader.png?branch=master

[release-image]: http://img.shields.io/badge/release-0.2.0-blue.svg?style=flat
[releases]: https://github.com/snowplow-incubator/snowplow-bigquery-loader

[techdocs-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/techdocs.png
[setup-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/setup.png
[contributing-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/contributing.png

[techdocs]: https://github.com/snowplow-incubator/snowplow-bigquery-loader/wiki
[setup]: https://github.com/snowplow-incubator/snowplow-bigquery-loader/wiki/Setup-guide
[contributing]: https://github.com/snowplow/snowplow/wiki/Contributing
