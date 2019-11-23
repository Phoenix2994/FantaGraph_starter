import graph.schema.SchemaBuilder;
import graph.population.PopulationBuilder;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class Main {
    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaBuilder.class);
    public static void main(String[] args) throws Exception {

        SchemaBuilder schemaBuilder = new SchemaBuilder();
        PopulationBuilder populationBuilder = new PopulationBuilder();

        //to create every time a new graph we drop the old one
        schemaBuilder.dropPreviousGraph();
        JanusGraph graph = JanusGraphFactory.open("conf/janusgraph-cassandra-elasticsearch.properties");
        final JanusGraphManagement management = graph.openManagement();

        schemaBuilder.createSchema(management);

        GraphTraversalSource g = graph.traversal();
        populationBuilder.populateGraph(g);

        //test if everything is ok
        Object vertex = g.V().has("player id", 2002).out("plays for").in("owns")
                .in("is commissioned by").out("trains").out("plays in").values("name").next();

        List<Map<Object, Object>> player_stats  =g.V().has("player id", 2002).out("stats")
                .valueMap().toList();

        double queryStart = System.currentTimeMillis();
        Object players = g.V().has("player id").values("name").toList();
        double queryEnd = System.currentTimeMillis();

        LOGGER.info(players.toString());
        LOGGER.info(String.valueOf(queryEnd - queryStart));
        LOGGER.info("TEST: LO stadium DOVREBBE ESSERE ARTEMIO FRANCHI E RISULTA " + vertex);
        LOGGER.info("STATISTICHE PLAYER 2002" + player_stats);
        g.tx().rollback();
        System.exit(0);
    }
}
