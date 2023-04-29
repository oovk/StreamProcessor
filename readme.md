# Car Stream Generator

This class generates a stream of random cars using the Apache Commons RDF library. The cars are generated with random values for the brand, plate, and speed properties. The stream can be configured to generate a specific number of cars per second, and can be started and stopped as needed.

## Usage

To use the `CarStreamGenerator`, simply create a new instance and call the `getStream` method to obtain a data stream:

```java
CarStreamGenerator generator = new CarStreamGenerator();
DataStream<Graph> stream = generator.getStream("http://test/stream");
```

To start and stop the stream, call the startStreaming and stopStreaming methods:

```java
generator.startStreaming();
Thread.sleep(10000); // Stream for 10 seconds
generator.stopStreaming();
```

# Car Stream Query Processor

This class processes a query that selects all BMW cars with their speed and plate information in a window of a specified duration. The input stream must contain RDF graphs representing cars with the `brand`, `plate`, and `speed` properties.

## Usage

To use the `CarStreamQueryProcessor`, create a new instance and pass in the input data stream and the duration of the window:

```java
CarStreamQueryProcessor processor = new CarStreamQueryProcessor(inputStream, Duration.ofSeconds(20));
String queryString = "SELECT ?car ?plate ?speed WHERE { ?car <http://example.org/vocabulary/brand> <http://example.org/cars/BMW> . ?car <http://example.org/vocabulary/plate> ?plate . ?car <http://example.org/vocabulary/speed> ?speed . }";
processor.processQuery(queryString);
```
The output of the query will be printed to the console.
