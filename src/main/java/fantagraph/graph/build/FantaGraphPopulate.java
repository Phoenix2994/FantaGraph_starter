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
                String name = (String) player_info.get("name");
                String team_name = (String) player_info.get("team");
                String player_place = (String) player_info.get("birthplace");
                String player_data = (String) player_info.get("birthdate");
                String player_nat = (String) player_info.get("nationality");
                String player_height = (String) player_info.get("height");
                String player_role = (String) player_info.get("role");
                String player_foot = (String) player_info.get("mainFoot");
                String player_img = (String) player_info.get("img");
                long player_id = Long.parseLong((String) player_info.get("id"));
                Long player_quot = (Long) player_info.get("quot");
                JSONObject player_stats = (JSONObject) player_info.get("stats");
                String prosecutor_name = (String) player_info.get("prosecutor");
                final Vertex player = g.addV("player").property("name", name)
                        .property("birthdate", player_data).property("birthplace", player_place)
                        .property("nationality", player_nat).property("height", player_height)
                        .property("role", player_role).property("mainFoot", player_foot)
                        .property("img", player_img).property("player id", player_id)
                        .property("quot", player_quot).next();
                Vertex team = g.V().hasLabel("team").has("name", (team_name + " ")
                        .split(" ")[0].toUpperCase()).next();
                g.V(player).as("a").V(team).addE("plays for").from("a").next();
                boolean exist = g.V().hasLabel("prosecutor").has("name", prosecutor_name).hasNext();
                for (Object season_key : player_stats.keySet()) {
                    JSONObject season_stats = (JSONObject) player_stats.get(season_key);
                    if (season_stats.containsKey("team")) {
                        String season_year = season_key.toString().replace("stagione", "");
                        String role = (String) season_stats.get("r");
                        String playing_team = (String) season_stats.get("team");
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
                        if (g.V().hasLabel("team").has("name", playing_team.toUpperCase())
                                .hasNext()) {
                            Vertex season_team = g.V().hasLabel("team").has("name",
                                    playing_team.toUpperCase()).next();
                            final Vertex player_season_stats = g.addV("season stats")
                                    .property("year", season_year)
                                    .property("role", role).property("played matches", played_matches)
                                    .property("average", avg_rating)
                                    .property("fanta average", avg_fantarating)
                                    .property("gauss average", avg_gaussian_rating)
                                    .property("gauss fanta average", avg_gaussian_fantarating)
                                    .property("scored goals", scored_goals)
                                    .property("conceded goals", conceded_goals)
                                    .property("assists", assists).property("yellow cards", yellow_cards)
                                    .property("red cards", red_cards).property("own goals", own_goals).next();
                            g.V(player).as("a").V(player_season_stats).addE("stats").from("a").next();
                            g.V(player_season_stats).as("a").V(season_team).addE("stats").from("a")
                                    .next();
                        } else {
                            final Vertex player_season_stats = g.addV("season stats")
                                    .property("year", season_year)
                                    .property("role", role).property("played matches", played_matches)
                                    .property("average", avg_rating)
                                    .property("fanta average", avg_fantarating)
                                    .property("gauss average", avg_gaussian_rating)
                                    .property("gauss fanta average", avg_gaussian_fantarating)
                                    .property("scored goals", scored_goals)
                                    .property("conceded goals", conceded_goals)
                                    .property("assists", assists).property("yellow cards", yellow_cards)
                                    .property("red cards", red_cards).property("own goals", own_goals)
                                    .property("team", playing_team.toUpperCase()).next();
                            g.V(player).as("a").V(player_season_stats).addE("stats").from("a").next();
                        }
                    }
                }
                final Vertex prosecutor;
                if (!exist) {
                    prosecutor_counter = prosecutor_counter + 1;
                    prosecutor = g.addV("prosecutor").property("name", prosecutor_name)
                            .property("prosecutor id", prosecutor_counter).next();
                } else {
                    prosecutor = g.V().hasLabel("prosecutor").has("name", prosecutor_name).next();
                }
                g.V(player).as("a").V(prosecutor).addE("is assisted by").from("a").next();
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
                final Vertex team = g.addV("team").property("name", name)
                        .property("logo", logo)
                        .property("team id", team_counter).next();
                //team vertex added to graph
                JSONObject stadium_info = (JSONObject) team_info.get("stadium");
                String stadium_name = (String) stadium_info.get("name");
                String stadium_place = (String) stadium_info.get("city");
                Long stadium_fans = (Long) stadium_info.get("capacity");
                String stadium_img = (String) stadium_info.get("img");
                boolean stadium_exist = g.V().hasLabel("stadium").has("name", stadium_name).hasNext();
                final Vertex stadium;
                if (!stadium_exist) {
                    stadium_counter = stadium_counter + 1;
                    stadium = g.addV("stadium").property("name", stadium_name)
                            .property("city", stadium_place).property("capacity", stadium_fans)
                            .property("img", stadium_img).property("stadium id", stadium_counter).next();
                } else {
                    stadium = g.V().hasLabel("stadium").has("name", stadium_name).next();
                }
                boolean league_exist = g.V().hasLabel("league")
                        .has("name", "Serie A TIM").hasNext();
                final Vertex league;
                if (!league_exist) {
                    league = g.addV("league").property("name", "Serie A TIM")
                            .property("country", "ITALIA").next();
                } else {
                    league = g.V().hasLabel("league").has("name", "Serie A TIM").next();
                }
                //stadium vertex added to graph
                JSONObject president_info = (JSONObject) team_info.get("president");
                String pres_name = (String) president_info.get("name");
                String pres_place = (String) president_info.get("birthplace");
                String pres_data = (String) president_info.get("birthdate");
                String pres_nat = (String) president_info.get("nationality");
                president_counter = president_counter + 1;
                final Vertex president = g.addV("president").property("name", pres_name)
                        .property("birthdate", pres_data.split(" ")[0])
                        .property("birthplace", pres_place)
                        .property("nationality", pres_nat).property("president id", president_counter).next();
                //president vertex added to graph
                JSONObject coach_info = (JSONObject) team_info.get("coach");
                String coach_name = (String) coach_info.get("name");
                String coach_place = (String) coach_info.get("birthplace");
                String coach_data = (String) coach_info.get("birthdate");
                String coach_nat = (String) coach_info.get("nationality");
                Long coach_schema = (Long) coach_info.get("module");
                coach_counter = coach_counter + 1;
                final Vertex coach = g.addV("coach").property("name", coach_name)
                        .property("birthdate", coach_data.split(" ")[0])
                        .property("birthplace", coach_place)
                        .property("nationality", coach_nat).property("module", coach_schema)
                        .property("coach id", coach_counter).next();
                //coach vertex added to graph
                g.V(team).as("a").V(stadium).addE("plays in").from("a").next();
                g.V(team).as("a").V(league).addE("participates").from("a").next();
                g.V(coach).as("a").V(team).addE("trains").from("a").next();
                g.V(coach).as("a").V(president).addE("is commissioned by").from("a").next();
                g.V(president).as("a").V(team).addE("owns").from("a").next();

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
