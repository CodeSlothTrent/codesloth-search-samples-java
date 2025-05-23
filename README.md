# OpenSearch Java Samples

This repository contains Java code samples for working with OpenSearch, demonstrating various features and capabilities of the OpenSearch Java client.

## Project Structure

- `OpenSearchSamples/` - Maven project containing the OpenSearch Java samples
  - `src/test/java/` - Test classes demonstrating OpenSearch features
    - `GettingStarted/` - Basic examples for getting started with OpenSearch
    - `KeywordDemo/` - Examples demonstrating keyword field mapping and searching
      - `KeywordIndexingTests.java` - Tests for keyword field indexing
      - `KeywordSearchingTests.java` - Tests for keyword field searching
      - `KeywordAggregationTests.java` - Tests for keyword field aggregations
      - `KeywordScriptingTests.java` - Tests for keyword field scripting
      - `KeywordSortingTests.java` - Tests for keyword field sorting
    - `TextDemo/` - Examples demonstrating text field mapping and analysis
      - `TextIndexingTests.java` - Tests for text field indexing
      - `TextTests.java` - Tests for text field analysis and searching
    - `TestExtensions/` - JUnit 5 extensions for managing OpenSearch resources
    - `TestInfrastructure/` - Helper classes for creating and managing test indices
  - `infrastructure/` - Docker Compose configuration for running OpenSearch locally

## Prerequisites

- Java 17 or later
- Maven
- Docker and Docker Compose

## Running the Tests

1. Start the OpenSearch container:

```bash
cd OpenSearchSamples/infrastructure
docker-compose up -d
```

2. Run the tests:

```bash
cd OpenSearchSamples
mvn test
```

## Test Categories

### Getting Started Tests

Basic tests demonstrating how to connect to OpenSearch, check cluster health, and create/delete indices.

### Keyword Demo Tests

Tests demonstrating how keyword fields work in OpenSearch:
- Keyword fields are indexed as a single token without analysis
- Exact matching with term queries
- Filtering with boolean queries
- Scoring with constant score queries
- Aggregations (terms, cardinality, top hits, field collapsing, adjacency matrix)
- Scripting (creating scripted fields, using parameters in scripts)
- Sorting (script-based sorting, field sorting, lexicographical vs. numerical sorting)

### Text Demo Tests

Tests demonstrating how text fields work in OpenSearch:
- Text fields are analyzed using the standard analyzer by default
- Configuring analyzers with stop words
- Configuring analyzers with maximum token length
- Searching text fields with term queries

## License

This project is licensed under the MIT License - see the LICENSE file for details.
