package solution;
import org.apache.commons.rdf.api.Graph;
import org.streamreasoning.rsp4j.abstraction.ContinuousProgram;
import org.streamreasoning.rsp4j.abstraction.QueryTaskAbstractionImpl;
import org.streamreasoning.rsp4j.abstraction.TaskAbstractionImpl;
import org.streamreasoning.rsp4j.api.querying.ContinuousQuery;
import org.streamreasoning.rsp4j.api.stream.data.DataStream;
import org.streamreasoning.rsp4j.yasper.querying.operators.r2r.Binding;
import org.streamreasoning.rsp4j.yasper.querying.syntax.TPQueryFactory;
import utils.CarStreamGenerator;

/***
 * We generate random cars, From the 20 cars generated in 20s window, we count how many of them are Audi A3 where A3 is model name.
 */
public class CarQueryProcessing {

    public static void main(String[] args) throws InterruptedException {
        // Set up the stream generator
        CarStreamGenerator generator = new CarStreamGenerator();
        DataStream<Graph> inputStream = generator.getStream("http://test/stream");
        generator.printStream(inputStream);

        // Define the query
        ContinuousQuery<Graph, Graph, Binding, Binding> query =
                TPQueryFactory.parse(
                        ""
                                + "REGISTER RSTREAM <http://out/stream> AS "
                                + "SELECT (COUNT(?car) AS ?AudiA3count) "
                                + "FROM NAMED WINDOW <http://test/window> ON <http://test/stream> [RANGE PT20S STEP PT1S] "
                                + "WHERE {"
                                + "   WINDOW <http://test/window> { " +
                                "?car <http://example.org/vocabulary/brand> <http://example.org/cars/Audi> ."+
                                "?car <http://example.org/vocabulary/model> <http://example.org/cars/A3> ." +
                                "}"
                                + "}");

        TaskAbstractionImpl<Graph, Graph, Binding, Binding> t =
                new QueryTaskAbstractionImpl.QueryTaskBuilder().fromQuery(query).build();
        ContinuousProgram<Graph, Graph, Binding, Binding> cp =
                new ContinuousProgram.ContinuousProgramBuilder()
                        .in(inputStream)
                        .addTask(t)
                        .out(query.getOutputStream())
                        .build();

        query.getOutputStream().addConsumer((el, ts) -> System.out.println(el + " @ " + ts));
        generator.startStreaming();
        Thread.sleep(20_000);
        generator.stopStreaming();
    }
}