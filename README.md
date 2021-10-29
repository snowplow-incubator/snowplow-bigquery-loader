# Snowplow BigQuery Loader

[![Build Status][build-image]][build-wf]
[![Release][release-image]][releases]
[![License][license-image]][license]

This project contains applications used to load [Snowplow][snowplow] enriched data into [Google BigQuery][bigquery].

## Quickstart

Assuming git and [SBT][sbt] installed:

```bash
$ git clone https://github.com/snowplow-incubator/snowplow-bigquery-loader
$ cd snowplow-bigquery-loader
$ sbt "project loader" test
$ sbt "project streamloader" test
$ sbt "project mutator" test
$ sbt "project repeater" test
```

## Benchmarks

This project comes with [sbt-jmh](https://github.com/ktoso/sbt-jmh).

To run a specific benchmark test:

```bash
$ sbt 'project benchmark' '+jmh:run -i 20 -wi 10 -f2 -t3 .*TransformAtomic.*'
```

Or, to run all benchmark tests (once more are added):

```bash
$ sbt 'project benchmark' '+jmh:run -i 20 -wi 10 -f2 -t3'
```

The number of warm-ups and iterations is what the `sbt-jmh` project recommends but they can be lowered for faster runs.

To see all `sbt-jmh` options: `jmh:run -h`.

Add new benchmarks to [this module](./benchmark/src/test/scala/com.snowplowanalytics.snowplow.storage.bigquery/benchmark/).

## Building fatjars

You can build the `jar` files for Mutator, Repeater and Streamloader with [sbt](https://www.scala-sbt.org/) like so:

```bash
$ sbt clean 'project mutator' assembly
$ sbt clean 'project repeater' assembly
$ sbt clean 'project streamloader' assembly
```

## Find out more

| **[Technical Docs][techdocs]**    | **[Setup Guide][setup]**    | **[Contributing][contributing]**          |
|-----------------------------------|-----------------------------|-------------------------------------------|
| [![i1][techdocs-image]][techdocs] | [![i2][setup-image]][setup] | [![i3][contributing-image]][contributing] |


## Copyright and License

Snowplow BigQuery Loader is copyright 2018-2021 Snowplow Analytics Ltd.

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

[build-image]: https://github.com/snowplow-incubator/snowplow-bigquery-loader/workflows/build/badge.svg
[build-wf]: https://github.com/snowplow-incubator/snowplow-bigquery-loader/actions?query=workflow%3Abuild

[release-image]: http://img.shields.io/badge/release-1.0.1-blue.svg?style=flat
[releases]: https://github.com/snowplow-incubator/snowplow-bigquery-loader

[techdocs-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/techdocs.png
[setup-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/setup.png
[contributing-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/contributing.png

[techdocs]: https://docs.snowplowanalytics.com/docs/setup-snowplow-on-gcp/setup-bigquery-destination/bigquery-loader-0-6-0/
[setup]: https://docs.snowplowanalytics.com/docs/setup-snowplow-on-gcp/setup-bigquery-destination/bigquery-loader-0-6-0/#setup-guide
[contributing]: https://github.com/snowplow/snowplow/wiki/Contributing
