package fantagraph.graph.build;

import org.janusgraph.core.*;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.janusgraph.core.schema.JanusGraphManagement;

import java.io.*;
import java.net.URL;

import java.util.Map;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

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
    }

    /**
     * Creates the properties for vertices, edges, and meta-properties.
     */
    private static void createProperties(final JanusGraphManagement management) {
        management.makePropertyKey("nome").dataType(String.class).make();
        management.makePropertyKey("nome squadra").dataType(String.class).make();
        management.makePropertyKey("nome giocatore").dataType(String.class).make();
        management.makePropertyKey("nome presidente").dataType(String.class).make();
        management.makePropertyKey("nome allenatore").dataType(String.class).make();
        management.makePropertyKey("nome stadio").dataType(String.class).make();
        management.makePropertyKey("nome procuratore").dataType(String.class).make();
        management.makePropertyKey("nome campionato").dataType(String.class).make();
        management.makePropertyKey("data nascita").dataType(String.class).make();
        management.makePropertyKey("luogo nascita").dataType(String.class).make();
        management.makePropertyKey("media eta").dataType(Double.class).make();
        management.makePropertyKey("rosa").dataType(Long.class).make();
        management.makePropertyKey("citta").dataType(String.class).make();
        management.makePropertyKey("capienza").dataType(Long.class).make();
        management.makePropertyKey("nazionalita").dataType(String.class).make();
        management.makePropertyKey("modulo").dataType(String.class).make();
        management.makePropertyKey("altezza").dataType(String.class).make();
        management.makePropertyKey("ruolo").dataType(String.class).make();
        management.makePropertyKey("piede").dataType(String.class).make();
        management.makePropertyKey("statistiche").dataType(String.class).make();
        management.makePropertyKey("img").dataType(String.class).make();
        management.makePropertyKey("id").dataType(String.class).make();
        management.makePropertyKey("paese").dataType(String.class).make();
    }

    private static void createSchema(final JanusGraphManagement management) {
        LOGGER.info("creating schema");
        createProperties(management);
        createVertexLabels(management);
        createEdgeLabels(management);
        buildCompositeIndex(management);
        management.commit();
    }

    private static void createPlayersVertices(GraphTraversalSource g) {
        JSONParser parser = new JSONParser();

        try {
            FantaGraphBuild main = new FantaGraphBuild();
            Object obj = parser.parse(new FileReader(main.getFileFromResources("players.txt")));
            JSONObject jsonObject = (JSONObject) obj;
            for (Object key : jsonObject.keySet()) {
                JSONObject player_info = (JSONObject) jsonObject.get(key);
                String name = (String) player_info.get("nome");
                String team_name = (String) player_info.get("squadra");
                String player_place = (String) player_info.get("luogo di nascita");
                String player_data = (String) player_info.get("data di nascita");
                String player_nat = (String) player_info.get("nazionalita");
                String player_height = (String) player_info.get("altezza");
                String player_role = (String) player_info.get("ruolo");
                String player_foot = (String) player_info.get("piede");
                String player_img = (String) player_info.get("img");
                String player_id = (String) player_info.get("id");
                String player_stats = player_info.get("statistiche").toString();
                String prosecutor_name = (String) player_info.get("procuratore");
                final Vertex player = g.addV("giocatore").property("nome giocatore", name).property("data nascita", player_data).
                        property("luogo nascita", player_place).property("nazionalita", player_nat).property("altezza", player_height).
                        property("ruolo", player_role).property("piede", player_foot).property("img", player_img).
                        property("id", player_id).property("statistiche", player_stats).next();
                Vertex team = g.V().hasLabel("squadra").has("nome squadra", (team_name + " ").split(" ")[0].toUpperCase()).next();
                g.V(player).as("a").V(team).addE("gioca per").from("a").next();
                boolean exist = g.V().hasLabel("procuratore").has("nome procuratore", prosecutor_name).hasNext();
                final Vertex prosecutor;
                if (!exist) {
                    prosecutor = g.addV("procuratore").property("nome procuratore", prosecutor_name).next();
                } else {
                    prosecutor = g.V().hasLabel("procuratore").has("nome procuratore", prosecutor_name).next();
                }
                g.V(player).as("a").V(prosecutor).addE("è assistito da").from("a").next();
                g.tx().commit();
            }
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            g.tx().rollback();
        }
    }

    private static void createInfosVertices(GraphTraversalSource g) {
        JSONParser parser = new JSONParser();
        try {
            FantaGraphBuild main = new FantaGraphBuild();
            Object obj = parser.parse(new FileReader(main.getFileFromResources("teams.txt")));

            JSONObject jsonObject = (JSONObject) obj;
            for (Object key : jsonObject.keySet()) {
                String name = (String) key;
                JSONObject team_info = (JSONObject) jsonObject.get(key);
                Double avg_age = (Double) team_info.get("media eta");
                Long team_members = (Long) team_info.get("rosa");
                final Vertex team = g.addV("squadra").property("nome squadra", name).property("rosa", team_members).property("media eta", avg_age).next();
                //team vertex added to graph
                JSONObject stadium_info = (JSONObject) team_info.get("stadio");
                String stadium_name = (String) stadium_info.get("nome");
                String stadium_place = (String) stadium_info.get("citta");
                Long stadium_fans = (Long) stadium_info.get("capienza");
                boolean stadium_exist = g.V().hasLabel("stadio").has("nome stadio", stadium_name).hasNext();
                final Vertex stadium;
                if (!stadium_exist) {
                    stadium = g.addV("stadio").property("nome stadio", stadium_name).property("citta", stadium_place).property("capienza", stadium_fans).next();
                } else {
                    stadium = g.V().hasLabel("stadio").has("nome stadio", stadium_name).next();
                }
                boolean league_exist = g.V().hasLabel("campionato").has("nome campionato", "Serie A TIM").hasNext();
                final Vertex league;
                if (!league_exist) {
                    league = g.addV("campionato").property("nome campionato", "Serie A TIM").property("paese", "ITALIA").next();
                } else {
                    league = g.V().hasLabel("campionato").has("nome campionato", "Serie A TIM").next();
                }
                //stadium vertex added to graph
                JSONObject president_info = (JSONObject) team_info.get("presidente");
                String pres_name = (String) president_info.get("nome");
                String pres_place = (String) president_info.get("luogo nascita");
                String pres_data = (String) president_info.get("data nascita");
                String pres_nat = (String) president_info.get("nazionalita");
                final Vertex president = g.addV("presidente").property("nome presidente", pres_name).property("data nascita", pres_data).property("luogo nascita", pres_place).property("nazionalita", pres_nat).next();
                //president vertex added to graph
                JSONObject coach_info = (JSONObject) team_info.get("allenatore");
                String coach_name = (String) coach_info.get("nome");
                String coach_place = (String) coach_info.get("luogo nascita");
                String coach_data = (String) coach_info.get("data nascita");
                String coach_nat = (String) coach_info.get("nazionalita");
                Long coach_schema = (Long) coach_info.get("modulo");
                final Vertex coach = g.addV("allenatore").property("nome allenatore", coach_name).property("data nascita", coach_data).property("luogo nascita", coach_place).property("nazionalita", coach_nat).property("modulo", coach_schema).next();
                //coach vertex added to graph
                g.V(team).as("a").V(stadium).addE("gioca in").from("a").next();
                g.V(team).as("a").V(league).addE("partecipa a").from("a").next();
                g.V(coach).as("a").V(team).addE("allena").from("a").next();
                g.V(coach).as("a").V(president).addE("è incaricato da").from("a").next();
                g.V(president).as("a").V(team).addE("possiede").from("a").next();

                g.tx().commit();
            }

        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            g.tx().rollback();
        }
    }


    private static void dropGraph(JanusGraph graph) throws Exception {
        if (graph != null) {
            JanusGraphFactory.drop(graph);
        }
    }

    private File getFileFromResources(String fileName) {

        ClassLoader classLoader = getClass().getClassLoader();

        URL resource = classLoader.getResource(fileName);
        if (resource == null) {
            throw new IllegalArgumentException("file is not found!");
        } else {
            return new File(resource.getFile());
        }

    }

    private static void buildCompositeIndex(JanusGraphManagement mgmt) {
        PropertyKey stadium_name = mgmt.getPropertyKey("nome stadio");
        JanusGraphManagement.IndexBuilder indexBuilder = mgmt.buildIndex("StadiumByName", Vertex.class).addKey(stadium_name);
        indexBuilder.buildCompositeIndex();
        PropertyKey team_name = mgmt.getPropertyKey("nome squadra");
        indexBuilder = mgmt.buildIndex("TeamByName", Vertex.class).addKey(team_name);
        indexBuilder.buildCompositeIndex();
        PropertyKey player_id = mgmt.getPropertyKey("id");
        indexBuilder = mgmt.buildIndex("PlayerById", Vertex.class).addKey(player_id);
        indexBuilder.buildCompositeIndex();
        PropertyKey coach_name = mgmt.getPropertyKey("nome allenatore");
        indexBuilder = mgmt.buildIndex("CoachByName", Vertex.class).addKey(coach_name);
        indexBuilder.buildCompositeIndex();
        PropertyKey president_name = mgmt.getPropertyKey("nome presidente");
        indexBuilder = mgmt.buildIndex("PresidentByName", Vertex.class).addKey(president_name);
        indexBuilder.buildCompositeIndex();
        PropertyKey prosecutor_name = mgmt.getPropertyKey("nome procuratore");
        indexBuilder = mgmt.buildIndex("ProsecutorByName", Vertex.class).addKey(prosecutor_name);
        indexBuilder.buildCompositeIndex();

    }

    public static void main(String[] args) throws Exception {
        //to create every time a new graph we drop the old one
        JanusGraph graph_old = JanusGraphFactory.open("conf/janusgraph-cassandra-elasticsearch.properties");
        dropGraph(graph_old);

        JanusGraph graph = JanusGraphFactory.open("conf/janusgraph-cassandra-elasticsearch.properties");
        final JanusGraphManagement management = graph.openManagement();

        createSchema(management);

        GraphTraversalSource g = graph.traversal();
        createInfosVertices(g);
        createPlayersVertices(g);

        //test if everything is ok
        Object vertex = g.V().has("id", "2002").out("gioca per").in("possiede")
                .in("è incaricato da").out("allena").out("gioca in").values("nome stadio").next();

        LOGGER.info("TEST: LO STADIO DOVREBBE ESSERE ARTEMIO FRANCHI E RISULTA " + vertex);
        System.exit(0);
    }
}

