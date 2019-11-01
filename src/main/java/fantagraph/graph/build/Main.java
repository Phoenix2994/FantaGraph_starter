package fantagraph.graph.build;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class Main {
    private static final Logger LOGGER = LoggerFactory.getLogger(FantaGraphBuild.class);
    public static void main(String[] args) throws Exception {

        FantaGraphBuild fantaGraphBuild = new FantaGraphBuild();
        FantaGraphPopulate fantaGraphPopulate = new FantaGraphPopulate();

        //to create every time a new graph we drop the old one
        fantaGraphBuild.dropPreviousGraph();
        JanusGraph graph = JanusGraphFactory.open("conf/janusgraph-cassandra-elasticsearch.properties");
        final JanusGraphManagement management = graph.openManagement();

        fantaGraphBuild.createSchema(management);

        GraphTraversalSource g = graph.traversal();
        fantaGraphPopulate.populateGraph(g);

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
        System.exit(0);
    }
}
