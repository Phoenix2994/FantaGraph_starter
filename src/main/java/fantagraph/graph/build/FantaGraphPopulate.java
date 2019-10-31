package fantagraph.graph.build;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.net.URL;

public class FantaGraphPopulate {
    private static final Logger LOGGER = LoggerFactory.getLogger(FantaGraphBuild.class);

    private static void createPlayersVertices(GraphTraversalSource g) {
        JSONParser parser = new JSONParser();
        long prosecutor_counter = 0;
        try {
            Object obj = parser.parse(new FileReader(new FantaGraphPopulate().
                    getFileFromResources("players.txt")));
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
                JSONObject player_stats = (JSONObject) player_info.get("statistiche");
                String prosecutor_name = (String) player_info.get("procuratore");
                final Vertex player = g.addV("giocatore").property("nome", name)
                        .property("data nascita", player_data).property("luogo nascita", player_place)
                        .property("nazionalita", player_nat).property("altezza", player_height)
                        .property("ruolo", player_role).property("piede", player_foot)
                        .property("img", player_img).property("id giocatore", player_id)
                        .property("quot", player_quot).next();
                Vertex team = g.V().hasLabel("squadra").has("nome", (team_name + " ")
                        .split(" ")[0].toUpperCase()).next();
                g.V(player).as("a").V(team).addE("gioca per").from("a").next();
                boolean exist = g.V().hasLabel("procuratore").has("nome", prosecutor_name).hasNext();
                for (Object season_key : player_stats.keySet()) {
                    JSONObject season_stats = (JSONObject) player_stats.get(season_key);
                    if (season_stats.containsKey("squadra")) {
                        String season_year = season_key.toString().replace("stagione", "");
                        String role = (String) season_stats.get("r");
                        String playing_team = (String) season_stats.get("squadra");
                        Long played_matches = (Long) season_stats.get("pg");
                        Double avg_rating = ((Number) season_stats.get("mv")).doubleValue();
                        Double avg_fantarating = ((Number) season_stats.get("mf")).doubleValue();
                        Double avg_gaussian_rating = ((Number) season_stats.get("mvt")).doubleValue();
                        Double avg_gaussian_fantarating = ((Number) season_stats.get("mft")).doubleValue();
                        Long scored_goals = (Long) season_stats.get("gf");
                        Long conceded_goals = (Long) season_stats.get("gs");
                        Long assists = (Long) season_stats.get("ass");
                        Long yellow_cards = (Long) season_stats.get("amm");
                        Long red_cards = (Long) season_stats.get("esp");
                        Long own_goals = (Long) season_stats.get("aut");
                        if (g.V().hasLabel("squadra").has("nome", playing_team.toUpperCase())
                                .hasNext()) {
                            Vertex season_team = g.V().hasLabel("squadra").has("nome",
                                    playing_team.toUpperCase()).next();
                            final Vertex player_season_stats = g.addV("statistiche stagione")
                                    .property("anno", season_year)
                                    .property("ruolo", role).property("partite giocate", played_matches)
                                    .property("media voto", avg_rating)
                                    .property("media fantavoto", avg_fantarating)
                                    .property("media voto gauss", avg_gaussian_rating)
                                    .property("media fantavoto gauss", avg_gaussian_fantarating)
                                    .property("gol fatti", scored_goals)
                                    .property("gol subiti", conceded_goals)
                                    .property("assist", assists).property("ammonizioni", yellow_cards)
                                    .property("espulsioni", red_cards).property("autogol", own_goals).next();
                            g.V(player).as("a").V(player_season_stats).addE("statistiche").from("a").next();
                            g.V(player_season_stats).as("a").V(season_team).addE("statistiche").from("a")
                                    .next();
                        } else {
                            final Vertex player_season_stats = g.addV("statistiche stagione")
                                    .property("anno", season_year)
                                    .property("ruolo", role).property("partite giocate", played_matches)
                                    .property("media voto", avg_rating)
                                    .property("media fantavoto", avg_fantarating)
                                    .property("media voto gauss", avg_gaussian_rating)
                                    .property("media fantavoto gauss", avg_gaussian_fantarating)
                                    .property("gol fatti", scored_goals)
                                    .property("gol subiti", conceded_goals)
                                    .property("assist", assists).property("ammonizioni", yellow_cards)
                                    .property("espulsioni", red_cards).property("autogol", own_goals)
                                    .property("squadra", playing_team.toUpperCase()).next();
                            g.V(player).as("a").V(player_season_stats).addE("statistiche").from("a").next();
                        }
                    }
                }
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
            Object obj = parser.parse(new FileReader(new FantaGraphPopulate()
                    .getFileFromResources("teams.txt")));
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
                final Vertex team = g.addV("squadra").property("nome", name)
                        .property("logo", logo)
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
                        .property("data nascita", pres_data.split(" ")[0])
                        .property("luogo nascita", pres_place)
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
                        .property("data nascita", coach_data.split(" ")[0])
                        .property("luogo nascita", coach_place)
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

    private File getFileFromResources(String fileName) {

        ClassLoader classLoader = getClass().getClassLoader();

        URL resource = classLoader.getResource(fileName);
        if (resource == null) {
            throw new IllegalArgumentException("file is not found!");
        } else {
            return new File(resource.getFile());
        }

    }

    protected void populateGraph(GraphTraversalSource g) {
        createInfosVertices(g);
        createPlayersVertices(g);
    }

}
