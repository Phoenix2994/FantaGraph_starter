package fantagraph.graph.build;

import org.janusgraph.core.*;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.janusgraph.core.schema.JanusGraphManagement;

public class FantaGraphBuild {
    private static final Logger LOGGER = LoggerFactory.getLogger(FantaGraphBuild.class);

    /**
     * Creates the vertex labels.
     */
    private static void createVertexLabels(final JanusGraphManagement management) {
        management.makeVertexLabel("player").make();
        management.makeVertexLabel("coach").make();
        management.makeVertexLabel("prosecutor").make();
        management.makeVertexLabel("president").make();
        management.makeVertexLabel("team").make();
        management.makeVertexLabel("stadium").make();
        management.makeVertexLabel("league").make();
        management.makeVertexLabel("season stats").make();
        management.makeVertexLabel("user").make();
        management.makeVertexLabel("fantateam").make();
    }

    /**
     * Creates the edge labels.
     */
    private static void createEdgeLabels(final JanusGraphManagement management) {
        management.makeEdgeLabel("plays for").make();
        management.makeEdgeLabel("plays in").make();
        management.makeEdgeLabel("trains").make();
        management.makeEdgeLabel("is assisted by").make();
        management.makeEdgeLabel("owns").make();
        management.makeEdgeLabel("is commissioned by").make();
        management.makeEdgeLabel("participates").make();
        management.makeEdgeLabel("stats").make();
        management.makeEdgeLabel("fanta plays for").make();
        management.makeEdgeLabel("fanta owns").make();
    }

    /**
     * Creates the properties for vertices, edges, and meta-properties.
     */
    private static void createProperties(final JanusGraphManagement management) {
        management.makePropertyKey("name").dataType(String.class).make();
        management.makePropertyKey("birthdate").dataType(String.class).make();
        management.makePropertyKey("birthplace").dataType(String.class).make();
        management.makePropertyKey("city").dataType(String.class).make();
        management.makePropertyKey("capacity").dataType(Long.class).make();
        management.makePropertyKey("nationality").dataType(String.class).make();
        management.makePropertyKey("module").dataType(String.class).make();
        management.makePropertyKey("height").dataType(String.class).make();
        management.makePropertyKey("role").dataType(String.class).make();
        management.makePropertyKey("mainFoot").dataType(String.class).make();
        management.makePropertyKey("img").dataType(String.class).make();
        management.makePropertyKey("player id").dataType(Long.class).make();
        management.makePropertyKey("coach id").dataType(Long.class).make();
        management.makePropertyKey("president id").dataType(Long.class).make();
        management.makePropertyKey("stadium id").dataType(Long.class).make();
        management.makePropertyKey("team id").dataType(Long.class).make();
        management.makePropertyKey("prosecutor id").dataType(Long.class).make();
        management.makePropertyKey("country").dataType(String.class).make();
        management.makePropertyKey("logo").dataType(String.class).make();
        management.makePropertyKey("quot").dataType(Long.class).make();
        management.makePropertyKey("year").dataType(String.class).make();
        management.makePropertyKey("played matches").dataType(Long.class).make();
        management.makePropertyKey("average").dataType(Double.class).make();
        management.makePropertyKey("fanta average").dataType(Double.class).make();
        management.makePropertyKey("gauss average").dataType(Double.class).make();
        management.makePropertyKey("gauss fanta average").dataType(Double.class).make();
        management.makePropertyKey("scored goals").dataType(Long.class).make();
        management.makePropertyKey("conceded goals").dataType(Long.class).make();
        management.makePropertyKey("assists").dataType(Long.class).make();
        management.makePropertyKey("yellow cards").dataType(Long.class).make();
        management.makePropertyKey("red cards").dataType(Long.class).make();
        management.makePropertyKey("own goals").dataType(Long.class).make();
        management.makePropertyKey("team").dataType(String.class).make();
        management.makePropertyKey("username").dataType(String.class).make();
        management.makePropertyKey("user id").dataType(Long.class).make();
        management.makePropertyKey("password").dataType(String.class).make();
        management.makePropertyKey("email").dataType(String.class).make();
        management.makePropertyKey("fantateam id").dataType(Long.class).make();
    }

    protected void dropPreviousGraph() throws Exception {
        JanusGraph graph_old = JanusGraphFactory.open("conf/janusgraph-cassandra-elasticsearch.properties");
        if (graph_old != null) {
            JanusGraphFactory.drop(graph_old);
        }
    }

    private static void buildCompositeIndex(JanusGraphManagement mgmt) {
        PropertyKey stadium_id = mgmt.getPropertyKey("stadium id");
        JanusGraphManagement.IndexBuilder indexBuilder = mgmt.buildIndex("StadiumById", Vertex.class)
                .addKey(stadium_id);
        indexBuilder.buildCompositeIndex();
        PropertyKey team_id = mgmt.getPropertyKey("team id");
        indexBuilder = mgmt.buildIndex("TeamById", Vertex.class).addKey(team_id);
        indexBuilder.buildCompositeIndex();
        PropertyKey player_id = mgmt.getPropertyKey("player id");
        indexBuilder = mgmt.buildIndex("PlayerById", Vertex.class).addKey(player_id);
        indexBuilder.buildCompositeIndex();
        PropertyKey coach_id = mgmt.getPropertyKey("coach id");
        indexBuilder = mgmt.buildIndex("CoachById", Vertex.class).addKey(coach_id);
        indexBuilder.buildCompositeIndex();
        PropertyKey president_id = mgmt.getPropertyKey("president id");
        indexBuilder = mgmt.buildIndex("PresidentById", Vertex.class).addKey(president_id);
        indexBuilder.buildCompositeIndex();
        PropertyKey prosecutor_id = mgmt.getPropertyKey("prosecutor id");
        indexBuilder = mgmt.buildIndex("ProsecutorById", Vertex.class).addKey(prosecutor_id);
        indexBuilder.buildCompositeIndex();
        PropertyKey user_id = mgmt.getPropertyKey("user id");
        indexBuilder = mgmt.buildIndex("UserById", Vertex.class).addKey(user_id);
        indexBuilder.buildCompositeIndex();
        PropertyKey fantateam_id = mgmt.getPropertyKey("fantateam id");
        indexBuilder = mgmt.buildIndex("FantateamById", Vertex.class).addKey(fantateam_id);
        indexBuilder.buildCompositeIndex();
    }

    protected void createSchema(final JanusGraphManagement management){
        LOGGER.info("creating schema");
        createProperties(management);
        createVertexLabels(management);
        createEdgeLabels(management);
        buildCompositeIndex(management);
        management.commit();
    }


}

