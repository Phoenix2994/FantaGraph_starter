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
        management.makeVertexLabel("giocatore").make();
        management.makeVertexLabel("allenatore").make();
        management.makeVertexLabel("procuratore").make();
        management.makeVertexLabel("presidente").make();
        management.makeVertexLabel("squadra").make();
        management.makeVertexLabel("stadio").make();
        management.makeVertexLabel("campionato").make();
        management.makeVertexLabel("statistiche stagione").make();
    }

    /**
     * Creates the edge labels.
     */
    private static void createEdgeLabels(final JanusGraphManagement management) {
        management.makeEdgeLabel("gioca per").make();
        management.makeEdgeLabel("gioca in").make();
        management.makeEdgeLabel("allena").make();
        management.makeEdgeLabel("è assistito da").make();
        management.makeEdgeLabel("possiede").make();
        management.makeEdgeLabel("è incaricato da").make();
        management.makeEdgeLabel("partecipa a").make();
        management.makeEdgeLabel("statistiche").make();
    }

    /**
     * Creates the properties for vertices, edges, and meta-properties.
     */
    private static void createProperties(final JanusGraphManagement management) {
        management.makePropertyKey("nome").dataType(String.class).make();
        management.makePropertyKey("data nascita").dataType(String.class).make();
        management.makePropertyKey("luogo nascita").dataType(String.class).make();
        management.makePropertyKey("citta").dataType(String.class).make();
        management.makePropertyKey("capienza").dataType(Long.class).make();
        management.makePropertyKey("nazionalita").dataType(String.class).make();
        management.makePropertyKey("modulo").dataType(String.class).make();
        management.makePropertyKey("altezza").dataType(String.class).make();
        management.makePropertyKey("ruolo").dataType(String.class).make();
        management.makePropertyKey("piede").dataType(String.class).make();
        management.makePropertyKey("img").dataType(String.class).make();
        management.makePropertyKey("id giocatore").dataType(Long.class).make();
        management.makePropertyKey("id allenatore").dataType(Long.class).make();
        management.makePropertyKey("id presidente").dataType(Long.class).make();
        management.makePropertyKey("id stadio").dataType(Long.class).make();
        management.makePropertyKey("id squadra").dataType(Long.class).make();
        management.makePropertyKey("id procuratore").dataType(Long.class).make();
        management.makePropertyKey("paese").dataType(String.class).make();
        management.makePropertyKey("logo").dataType(String.class).make();
        management.makePropertyKey("quot").dataType(Long.class).make();
        management.makePropertyKey("anno").dataType(String.class).make();
        management.makePropertyKey("partite giocate").dataType(Long.class).make();
        management.makePropertyKey("media voto").dataType(Double.class).make();
        management.makePropertyKey("media fantavoto").dataType(Double.class).make();
        management.makePropertyKey("media voto gauss").dataType(Double.class).make();
        management.makePropertyKey("media fantavoto gauss").dataType(Double.class).make();
        management.makePropertyKey("gol fatti").dataType(Long.class).make();
        management.makePropertyKey("gol subiti").dataType(Long.class).make();
        management.makePropertyKey("assist").dataType(Long.class).make();
        management.makePropertyKey("ammonizioni").dataType(Long.class).make();
        management.makePropertyKey("espulsioni").dataType(Long.class).make();
        management.makePropertyKey("autogol").dataType(Long.class).make();
        management.makePropertyKey("squadra").dataType(String.class).make();
    }

    protected void dropPreviousGraph() throws Exception {
        JanusGraph graph_old = JanusGraphFactory.open("conf/janusgraph-cassandra-elasticsearch.properties");
        if (graph_old != null) {
            JanusGraphFactory.drop(graph_old);
        }
    }

    private static void buildCompositeIndex(JanusGraphManagement mgmt) {
        PropertyKey stadium_id = mgmt.getPropertyKey("id stadio");
        JanusGraphManagement.IndexBuilder indexBuilder = mgmt.buildIndex("StadiumById", Vertex.class)
                .addKey(stadium_id);
        indexBuilder.buildCompositeIndex();
        PropertyKey team_id = mgmt.getPropertyKey("id squadra");
        indexBuilder = mgmt.buildIndex("TeamById", Vertex.class).addKey(team_id);
        indexBuilder.buildCompositeIndex();
        PropertyKey player_id = mgmt.getPropertyKey("id giocatore");
        indexBuilder = mgmt.buildIndex("PlayerById", Vertex.class).addKey(player_id);
        indexBuilder.buildCompositeIndex();
        PropertyKey coach_id = mgmt.getPropertyKey("id allenatore");
        indexBuilder = mgmt.buildIndex("CoachById", Vertex.class).addKey(coach_id);
        indexBuilder.buildCompositeIndex();
        PropertyKey president_id = mgmt.getPropertyKey("id presidente");
        indexBuilder = mgmt.buildIndex("PresidentById", Vertex.class).addKey(president_id);
        indexBuilder.buildCompositeIndex();
        PropertyKey prosecutor_id = mgmt.getPropertyKey("id procuratore");
        indexBuilder = mgmt.buildIndex("ProsecutorById", Vertex.class).addKey(prosecutor_id);
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

