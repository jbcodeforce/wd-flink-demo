# A simple Confluent Cloud Flink SQL

## Scope

* Process leads for marketing campaigns. Raw data is lead activity, like clicks, open-email...
* There are multiple feeds of data to get lead information.

## Demonstration
* How to process schemaless topic as raw of byte (JSON payload)
* How to process CDC records with Debezium envelop
* Deduplication, filtering logic to create bronze layer
* Build Clicks, Lead and marketing campaing dimensions
* Build a fact for marketing program or activity tracking.

## Build

* [ ] From the domain classes build model.py we can develop kafka producer and consumer
* [ ] Raw tables are created in pipelines/raw for the 3 tables - few insert statement to get controlled data
* [ ] src_* tables created as bronze layer to
* [ ] Develop dim and facts


## Notes

* [See other demo for diagrams](https://jbcodeforce.github.io/healthcare-shift-left-demo/) and reusable approach if we want to extend from the current base
