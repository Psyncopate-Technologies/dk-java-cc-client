# Java Client

This project contains a Java application that subscribes to a topic on a Confluent Cloud Kafka cluster and sends a sample message, then consumes it and prints the consumed record to the console.

## Prerequisites

This project assumes you have [Java 21](https://www.oracle.com/java/technologies/downloads/#java21) and [Maven 3.9+](https://maven.apache.org/install.html) installed.

## Installation

You can compile this project by running the following command in the root directory of this project:

```shell
mvn package
```

## Usage

Export your Azure app client secret, then run the application:

```shell
export AZURE_CLIENT_SECRET=...
mvn exec:java
```

## Learn more

- For the Java client API, check out the [kafka-clients documentation](https://docs.confluent.io/platform/current/clients/javadocs/javadoc/index.html)
- Check out the full [getting started tutorial](https://developer.confluent.io/get-started/java/)
