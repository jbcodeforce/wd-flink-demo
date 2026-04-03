# A simple Confluent Cloud Flink SQL demonstration for marketing campaign processing

## Scope

* Process leads for marketing campaigns. Raw data include lead, activities, like clicks, open-email...
* There are multiple feeds of data to get lead information.

* A lead is the primary business identity. This class handles the core user's profile and custom attributes. m_id is the primary key
* The  data feeds are often just a long stream of activities (Email Opens, Web Visits, Form Fills). The `ActivityRecord` captures the "What" and "When.". It references a lead, while the primary key is a activity_id

Here is a simple figure of the pipeline architecture:

![](./docs/pipeline-view.drawio.png)

### Demonstration

The demonstration addresses the following standard patterns of data processing:

* How to process schemaless topic as raw of byte (JSON payload)
* How to process CDC records with Debezium envelop
* Deduplication, filtering logic to create bronze layer
* Build Clicks, Lead and marketing campaing dimensions
* Build a fact for marketing program or activity tracking.

## Build

* [ ] From the domain classes build model.py we can develop kafka producer for activities and consumer for fact tables
* [ ] Raw tables are created in pipelines/raw for the 3 tables. Leads is a CDC Debezium envelop. Marketing Program is a schemaless with payload-json field, and raw_activities record is a table with typed columns. For each table few insert statements are done to get useful data.
* [ ] src_* tables created as bronze layer to
* [ ] Develop dim and facts


## Notes

* [See other demo for diagrams](https://jbcodeforce.github.io/healthcare-shift-left-demo/) and reusable approach if we want to extend from the current base

## Demonstration Script

### Review the raw_mkt_pgm as schemaless

The unique field is a json string as payload (See []()). The json represents 
