package football.janusgraph.example;

import org.janusgraph.core.*;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.janusgraph.core.schema.JanusGraphManagement;

import java.io.FileReader;
import java.util.List;
import java.util.Map;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

public class FootballExample {
    private static final Logger LOGGER = LoggerFactory.getLogger(FootballExample.class);

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
    }

    /**
     * Creates the edge labels.
     */
    private static void createEdgeLabels(final JanusGraphManagement management) {
        management.makeEdgeLabel("gioca_per").make();
        management.makeEdgeLabel("gioca_in").make();
        management.makeEdgeLabel("allena").make();
        management.makeEdgeLabel("è_assistito_da").make();
        management.makeEdgeLabel("possiede").make();
        management.makeEdgeLabel("ha_assunto").make();
    }

    /**
     * Creates the properties for vertices, edges, and meta-properties.
     */
    private static void createProperties(final JanusGraphManagement management) {
        management.makePropertyKey("nome").dataType(String.class).make();
        management.makePropertyKey("data_nascita").dataType(String.class).make();
        management.makePropertyKey("luogo_nascita").dataType(String.class).make();
        management.makePropertyKey("media_eta").dataType(String.class).make();
        management.makePropertyKey("rosa").dataType(String.class).make();
        management.makePropertyKey("citta").dataType(String.class).make();
        management.makePropertyKey("capienza").dataType(String.class).make();
        management.makePropertyKey("nazionalita").dataType(String.class).make();
        management.makePropertyKey("modulo").dataType(String.class).make();
    }

    private static void createSchema(final JanusGraphManagement management) {
        LOGGER.info("creating schema");
        createProperties(management);
        createVertexLabels(management);
        createEdgeLabels(management);
        management.commit();
    }

    /*
    static public void createVertex(GraphTraversalSource g) {
        JSONParser parser = new JSONParser();
        try {
            Object obj = parser.parse(new FileReader("path"));
            JSONObject jsonObject = (JSONObject) obj;
            for (Iterator iterator = jsonObject.keySet().iterator(); iterator.hasNext(); ) {
                String key = (String) iterator.next();
                JSONObject player_info = (JSONObject) jsonObject.get(key);
                String nome = (String) player_info.get("nome");
                String squadra = (String) player_info.get("squadra");
                String prosecutor = (String) player_info.get("prosecutor");
                final Vertex player = g.addV("giocatore").property("nome", nome).next();
                final Vertex prosec = g.addV("procuratore").property("nome", prosecutor).next();
                g.V(player).as("a").V(squadra).addE("gioca_per").from("a").next();
                g.V(player).as("a").V(prosec).addE("è_assistito_da").from("a").next();
            }
            g.tx().commit();
        } catch (Exception e) {

        }
    }
*/
    private static void createTeamVertex(GraphTraversalSource g) {
        JSONParser parser = new JSONParser();
        try {
            Object obj = parser.parse(new FileReader("C:\\Student\\SWAM\\Project\\stats\\teams.txt"));
            JSONObject jsonObject = (JSONObject) obj;
            for (Object key : jsonObject.keySet()) {
                String name = (String) key;
                JSONObject team_info = (JSONObject) jsonObject.get(key);
                Double avg_age = (Double) team_info.get("media eta");
                Long team_members = (Long) team_info.get("rosa");
                final Vertex team = g.addV("squadra").property("nome", name).property("rosa", team_members).property("media eta", avg_age).next();
                //team vertex added to graph
                JSONObject stadium_info = (JSONObject) team_info.get("stadio");
                String stadium_name = (String) stadium_info.get("nome");
                String stadium_place = (String) stadium_info.get("citta");
                Long stadium_fans = (Long) stadium_info.get("capienza");
                boolean exist = g.V().hasLabel("stadio").has("nome", stadium_name).hasNext();
                final Vertex stadium;
                if (!exist) {
                    stadium = g.addV("stadio").property("nome", stadium_name).property("citta", stadium_place).property("capienza", stadium_fans).next();
                } else {
                    stadium = g.V().hasLabel("stadio").has("nome", stadium_name).next();
                }
                //stadium vertex added to graph
                JSONObject president_info = (JSONObject) team_info.get("presidente");
                String pres_name = (String) president_info.get("nome");
                String pres_place = (String) president_info.get("luogo nascita");
                String pres_data = (String) president_info.get("data nascita");
                String pres_nat = (String) president_info.get("nazionalita");
                final Vertex president = g.addV("presidente").property("nome", pres_name).property("data_nascita", pres_data).property("luogo_nascita", pres_place).property("nazionalita", pres_nat).next();
                //president vertex added to graph
                JSONObject coach_info = (JSONObject) team_info.get("allenatore");
                String coach_name = (String) coach_info.get("nome");
                String coach_place = (String) coach_info.get("luogo nascita");
                String coach_data = (String) coach_info.get("data nascita");
                String coach_nat = (String) coach_info.get("nazionalita");
                Long coach_schema = (Long) coach_info.get("modulo");
                final Vertex coach = g.addV("allenatore").property("nome", coach_name).property("data_nascita", coach_data).property("luogo_nascita", coach_place).property("nazionalita", coach_nat).property("modulo", coach_schema).next();
                //coach vertex added to graph
                g.V(team).as("a").V(stadium).addE("gioca_in").from("a").next();
                g.V(coach).as("a").V(team).addE("allena").from("a").next();
                g.V(president).as("a").V(coach).addE("ha_assunto").from("a").next();
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

    public static void main(String[] args) throws Exception {
        //to create every time a new graph we drop the old one
        JanusGraph graph_old = JanusGraphFactory.open("conf/janusgraph-cassandra-elasticsearch.properties");
        dropGraph(graph_old);

        JanusGraph graph = JanusGraphFactory.open("conf/janusgraph-cassandra-elasticsearch.properties");
        final JanusGraphManagement management = graph.openManagement();
        createSchema(management);

        GraphTraversalSource g = graph.traversal();
        createTeamVertex(g);

        //search one president from team
        Map<Object, Object> presVertex = g.V().hasLabel("squadra").has("nome", "PARMA").in("possiede").valueMap().next();
        //search one team from president passing through coach
        Map<Object, Object> teamVertex = g.V().hasLabel("presidente").has("nome", "GIORGIO SQUINZI").out("ha_assunto").out("allena").valueMap().next();
        //check the ROMA stadium is shared
        List<Map<Object, Object>> stadium_fans = g.V().hasLabel("squadra").has("nome", "ROMA").out("gioca_in").in("gioca_in").valueMap().toList();
        Object teams = g.V().hasLabel("stadio").has("nome", "OLIMPICO").in("gioca_in").values("nome").toList();

        LOGGER.info(presVertex.toString());
        LOGGER.info(teamVertex.toString());
        LOGGER.info(stadium_fans.toString());
        LOGGER.info(teams.toString());
        System.exit(0);
    }
}

