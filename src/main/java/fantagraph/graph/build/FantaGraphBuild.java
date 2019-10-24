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
        management.makePropertyKey("data nascita").dataType(String.class).make();
        management.makePropertyKey("luogo nascita").dataType(String.class).make();
        management.makePropertyKey("citta").dataType(String.class).make();
        management.makePropertyKey("capienza").dataType(Long.class).make();
        management.makePropertyKey("nazionalita").dataType(String.class).make();
        management.makePropertyKey("modulo").dataType(String.class).make();
        management.makePropertyKey("altezza").dataType(String.class).make();
        management.makePropertyKey("ruolo").dataType(String.class).make();
        management.makePropertyKey("piede").dataType(String.class).make();
        management.makePropertyKey("statistiche").dataType(String.class).make();
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
            long prosecutor_counter = 0;
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
                long player_id = Long.parseLong((String) player_info.get("id"));
                Long player_quot = (Long) player_info.get("quot");
                String player_stats = player_info.get("statistiche").toString();
                String prosecutor_name = (String) player_info.get("procuratore");
                final Vertex player = g.addV("giocatore").property("nome", name)
                        .property("data nascita", player_data).property("luogo nascita", player_place)
                        .property("nazionalita", player_nat).property("altezza", player_height)
                        .property("ruolo", player_role).property("piede", player_foot)
                        .property("img", player_img).property("id giocatore", player_id)
                        .property("statistiche", player_stats).property("quot", player_quot).next();
                Vertex team = g.V().hasLabel("squadra").has("nome", (team_name + " ")
                        .split(" ")[0].toUpperCase()).next();
                g.V(player).as("a").V(team).addE("gioca per").from("a").next();
                boolean exist = g.V().hasLabel("procuratore").has("nome", prosecutor_name).hasNext();
                final Vertex prosecutor;
                if (!exist) {
                    prosecutor_counter = prosecutor_counter + 1;
                    prosecutor = g.addV("procuratore").property("nome", prosecutor_name)
                            .property("id procuratore", prosecutor_counter).next();
                } else {
                    prosecutor = g.V().hasLabel("procuratore").has("nome", prosecutor_name).next();
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
            long team_counter = 0;
            long president_counter = 0;
            long coach_counter = 0;
            long stadium_counter = 0;
            JSONObject jsonObject = (JSONObject) obj;
            for (Object key : jsonObject.keySet()) {
                String name = (String) key;
                JSONObject team_info = (JSONObject) jsonObject.get(key);
                String logo = (String) team_info.get("logo");
                team_counter = team_counter + 1;
                final Vertex team = g.addV("squadra").property("nome", name).property("logo", logo)
                        .property("id squadra", team_counter).next();
                //team vertex added to graph
                JSONObject stadium_info = (JSONObject) team_info.get("stadio");
                String stadium_name = (String) stadium_info.get("nome");
                String stadium_place = (String) stadium_info.get("citta");
                Long stadium_fans = (Long) stadium_info.get("capienza");
                String stadium_img = (String) stadium_info.get("img");
                boolean stadium_exist = g.V().hasLabel("stadio").has("nome", stadium_name).hasNext();
                final Vertex stadium;
                if (!stadium_exist) {
                    stadium_counter = stadium_counter + 1;
                    stadium = g.addV("stadio").property("nome", stadium_name)
                            .property("citta", stadium_place).property("capienza", stadium_fans)
                            .property("img", stadium_img).property("id stadio", stadium_counter).next();
                } else {
                    stadium = g.V().hasLabel("stadio").has("nome", stadium_name).next();
                }
                boolean league_exist = g.V().hasLabel("campionato")
                        .has("nome", "Serie A TIM").hasNext();
                final Vertex league;
                if (!league_exist) {
                    league = g.addV("campionato").property("nome", "Serie A TIM")
                            .property("paese", "ITALIA").next();
                } else {
                    league = g.V().hasLabel("campionato").has("nome", "Serie A TIM").next();
                }
                //stadium vertex added to graph
                JSONObject president_info = (JSONObject) team_info.get("presidente");
                String pres_name = (String) president_info.get("nome");
                String pres_place = (String) president_info.get("luogo nascita");
                String pres_data = (String) president_info.get("data nascita");
                String pres_nat = (String) president_info.get("nazionalita");
                president_counter = president_counter + 1;
                final Vertex president = g.addV("presidente").property("nome", pres_name)
                        .property("data nascita", pres_data).property("luogo nascita", pres_place)
                        .property("nazionalita", pres_nat).property("id presidente", president_counter).next();
                //president vertex added to graph
                JSONObject coach_info = (JSONObject) team_info.get("allenatore");
                String coach_name = (String) coach_info.get("nome");
                String coach_place = (String) coach_info.get("luogo nascita");
                String coach_data = (String) coach_info.get("data nascita");
                String coach_nat = (String) coach_info.get("nazionalita");
                Long coach_schema = (Long) coach_info.get("modulo");
                coach_counter = coach_counter + 1;
                final Vertex coach = g.addV("allenatore").property("nome", coach_name)
                        .property("data nascita", coach_data).property("luogo nascita", coach_place)
                        .property("nazionalita", coach_nat).property("modulo", coach_schema)
                        .property("id allenatore", coach_counter).next();
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
        Object vertex = g.V().has("id giocatore", 2002).out("gioca per").in("possiede")
                .in("è incaricato da").out("allena").out("gioca in").values("nome").next();

        double queryStart = System.currentTimeMillis();
        Object players = g.V().has("id giocatore").values("nome").toList();
        double queryEnd = System.currentTimeMillis();

        LOGGER.info(players.toString());
        LOGGER.info(String.valueOf(queryEnd - queryStart));
        LOGGER.info("TEST: LO STADIO DOVREBBE ESSERE ARTEMIO FRANCHI E RISULTA " + vertex);
        System.exit(0);
    }
}

