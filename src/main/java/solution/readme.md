# Car Query Processing

This program generates a stream of random cars and processes a query that selects all BMW cars with their speed and plate information in a 20-second window. It uses the RSP4J framework to perform continuous query processing.

## Requirements

- Java 8 or later
- Maven 3.5 or later

## How to run

1. Clone this repository
2. Run `mvn clean install` to build the project
3. Run `mvn exec:java -Dexec.mainClass="solution.CarQueryProcessing"` to start the program

The program will start generating a stream of random cars and processing the query for 40 seconds. The results will be printed to the console.

## Program details

The program uses the `CarStreamGenerator` class to generate a stream of random cars. It then defines a query using the `TPQueryFactory` class that selects all BMW cars with their speed and plate information in a 20-second window. The query is processed using the `TaskAbstractionImpl` and `ContinuousProgram` classes provided by the RSP4J framework. Finally, the program starts the stream generator and processes the query for 40 seconds, printing the results to the console.
