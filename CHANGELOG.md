# Changelog

## 2.1.0.7 (January 22, 2018)

* Supervise mode fixed

## 2.1.0.6 (December 21, 2017)

* Added validation to curl parameters
* Added mesos constraints management to spark driver
* Added a secure way to retrieve user and passwords information from vault
* Added Tenant Name variable being able to have Spark Dispatcher with different name

## 2.1.0.5 (November 07, 2017)

* Connection to Elastic with TLS
* Connection to Postgres with TLS, unified in datastore identity
* Removed Kafka identity, unified in datastore identity
* Removed script connection to Postgres 
* Removed Mesos secret and principal from curls
* Added configurable HDFS timeout

## 2.1.0.4 (August 17, 2017)

* Spark Dispatcher retrieves Mesos Principal and Secret from Vault

## 2.1.0.3 (July 26, 2017)

* Fix History Server env vars


## 2.1.0.2 (July 25, 2017)

* Dynamic Authentication for History Server
* SDN compatibility and isolation for History Server


## 2.1.0.1 (July 18, 2017)

* Refactor vault variables


## 2.1.0.0 (July 13, 2017)

* Spark-vault interactions
* SDN compatibility and isolation
* Kerberized access to hdfs
* Postgress TLS connection
* Dynamic Authentication
* Stratio Mesos security compatibility
* Initial Stratio Version of Spark
* Forked from Apache Spark 2.1.0
