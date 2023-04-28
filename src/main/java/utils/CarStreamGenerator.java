package utils;
import org.apache.commons.rdf.api.Graph;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.RDF;
import org.streamreasoning.rsp4j.api.RDFUtils;
import org.streamreasoning.rsp4j.api.stream.data.DataStream;
import org.streamreasoning.rsp4j.yasper.examples.RDFStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class CarStreamGenerator {
    private static final String PREFIX = "http://example.org/cars/";
    private static final Long TIMEOUT = 1000L;

    private final String[] carBrands = new String[]{"Mercedes-Benz", "Audi", "BMW", "Ford", "Toyota"};
    private final String[] carModels = new String[]{"C-Class", "A3", "3-Series", "Mustang", "Corolla"};
    private final Map<String, DataStream<Graph>> activeStreams;
    private final AtomicBoolean isStreaming;
    private final Random randomGenerator;
    private AtomicLong streamIndexCounter;

    public CarStreamGenerator() {
        this.streamIndexCounter = new AtomicLong(0);
        this.activeStreams = new HashMap<>();
        this.isStreaming = new AtomicBoolean(false);
        this.randomGenerator = new Random(1337);
    }

    public static String getPREFIX() {
        return CarStreamGenerator.PREFIX;
    }

    public DataStream<Graph> getStream(String streamURI) {
        if (!activeStreams.containsKey(streamURI)) {
            RDFStream stream = new RDFStream(streamURI);
            activeStreams.put(streamURI, stream);
        }
        return activeStreams.get(streamURI);
    }

    public void startStreaming() {
        if (!this.isStreaming.get()) {
            this.isStreaming.set(true);
            Runnable task = () -> {
                long ts = 0;
                while (this.isStreaming.get()) {
                    long finalTs = ts;
                    activeStreams.entrySet().forEach(e -> generateDataAndAddToStream(e.getValue(), finalTs));
                    ts += TIMEOUT;
                    try {
                        Thread.sleep(TIMEOUT);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            };

            Thread thread = new Thread(task);
            thread.start();
        }
    }

    private void generateDataAndAddToStream(DataStream<Graph> stream, long ts) {
        RDF instance = RDFUtils.getInstance();
        Graph graph = instance.createGraph();
        IRI brand = instance.createIRI("http://example.org/vocabulary/brand");
        IRI model = instance.createIRI("http://example.org/vocabulary/model");

        graph.add(instance.createTriple(
                instance.createIRI(getPREFIX() + "car" + streamIndexCounter.incrementAndGet()),
                brand,
                instance.createIRI(getPREFIX() + selectRandomCarBrand())
        ));

        graph.add(instance.createTriple(
                instance.createIRI(getPREFIX() + "car" + streamIndexCounter.get()),
                model,
                instance.createIRI(getPREFIX() + selectRandomCarModel())
        ));

        stream.put(graph, ts);
    }

    private String selectRandomCarBrand() {
        int randomIndex = randomGenerator.nextInt(carBrands.length);
        return carBrands[randomIndex];
    }

    private String selectRandomCarModel() {
        int randomIndex = randomGenerator.nextInt(carModels.length);
        return carModels[randomIndex];
    }

    public void stopStreaming() {
        this.isStreaming.set(false);
    }

    public static void printStream(DataStream<Graph> stream) {
        stream.addConsumer((graph, timestamp) -> {
            System.out.println("Stream is currently streaming at timestamp " + timestamp);
            graph.stream().forEach(triple -> {
                System.out.println(triple.getSubject() + " " + triple.getPredicate() + " " + triple.getObject());
            });
        });
    }

    public static void main(String[] args){
        CarStreamGenerator generator = new CarStreamGenerator();
        generator.startStreaming();
        DataStream<Graph> inputStream = generator.getStream("http://example.org/cars/");
        generator.printStream(inputStream);

    }
}

