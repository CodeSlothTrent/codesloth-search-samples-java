# OpenSearch Java Samples

This repository contains Java code samples for working with OpenSearch, demonstrating various features and capabilities of the OpenSearch Java client through comprehensive test suites to accompany blog posts at https://codesloth.blog.

## Project Structure

- `OpenSearchSamples/` - Maven project containing OpenSearch Java samples organized as JUnit test classes
  - `src/test/java/` - Test suites exploring OpenSearch field types and core capabilities
  - `TestExtensions/` - JUnit 5 extensions for managing OpenSearch test resources
  - `TestInfrastructure/` - Helper utilities for test index management

## Prerequisites

- Java 17 or later
- Maven
- Docker (for Testcontainers)

## Running the Tests

The tests use Testcontainers to automatically provision OpenSearch instances, so no manual setup is required:

```bash
cd OpenSearchSamples
mvn test
```

Testcontainers will automatically:
- Download the OpenSearch Docker image (if not already cached)
- Start an OpenSearch container
- Run the tests
- Clean up the container after tests complete

## Test Categories

### Getting Started

Foundational tests that demonstrate basic OpenSearch operations including cluster connectivity, health checks, and index lifecycle management.

### Field Type Demos

Each field type demo comprehensively explores how that data type works across OpenSearch's core capabilities:

#### Keyword Fields

Explores keyword field behavior across indexing, searching, aggregations, sorting, and scripting. Keyword fields store exact string values without analysis, making them ideal for structured data like IDs, categories, and tags.

#### Text Fields

Demonstrates text field capabilities including full-text search, analyzer configuration, and tokenization behavior. Text fields are analyzed and optimized for natural language search queries.

#### Byte Fields

Examines byte field usage for storing small integer values (-128 to 127) with tests covering indexing, range queries, aggregations, and numeric operations.

#### Short Fields

Investigates short field functionality for storing integer values (-32,768 to 32,767) including indexing patterns, numeric queries, aggregations, and sorting behavior.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
