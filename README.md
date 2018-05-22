# Snowplow BigQuery Loader

DataflowRunner application loading enriched data from Google Pubsub into Bigquery.

## Usage

```bash
$ cd snowplow-bigquery-loader && sbt
$ run --input=projects/snowplow-project/topics/snplow-enriched-good --project-id=snowplow-project --dataset-id=snowplow-dataset --table-id=events
```

To build package run:

```bash
$ sbt pack
```

### REPL

To experiment with current codebase in [Scio REPL](https://github.com/spotify/scio/wiki/Scio-REPL)
simply:

```
sbt repl/run
```

