package graph.population;

import graph.schema.SchemaBuilder;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utilities.labels.Branch;
import utilities.labels.Node;
import utilities.labels.Property;

import java.io.File;
import java.io.FileReader;
import java.net.URL;

public class PopulationBuilder {
    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaBuilder.class);

    private static void createPlayersVertices(GraphTraversalSource g) {
        JSONParser parser = new JSONParser();
        long prosecutor_counter = 0;
        try {
            Object obj = parser.parse(new FileReader(new PopulationBuilder().
                    getFileFromResources("players.txt")));
            JSONObject jsonObject = (JSONObject) obj;
            for (Object key : jsonObject.keySet()) {

                //GET Player Infos from file
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

                // ADD Player vertex
                final Vertex player = g.addV(Node.PLAYER).property(Property.NAME[0], name)
                        .property(Property.BIRTHDATE[0], player_data).property(Property.BIRTHPLACE[0], player_place)
                        .property(Property.NATIONALITY[0], player_nat).property(Property.HEIGHT[0], player_height)
                        .property(Property.ROLE[0], player_role).property(Property.MAIN_FOOT[0], player_foot)
                        .property(Property.IMG[0], player_img).property(Property.PLAYER_ID[0], player_id)
                        .property(Property.QUOT[0], player_quot).next();

                // ADD Player-Team edge
                Vertex team = g.V().hasLabel(Node.TEAM).has(Property.NAME[0], (team_name + " ")
                        .split(" ")[0].toUpperCase()).next();
                g.V(player).as("a").V(team).addE(Branch.PLAYER_TO_TEAM).from("a").next();

                // GET player stats from file
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
                        if (g.V().hasLabel(Node.TEAM).has(Property.NAME[0], playing_team.toUpperCase())
                                .hasNext()) {
                            Vertex season_team = g.V().hasLabel(Node.TEAM).has(Property.NAME[0],
                                    playing_team.toUpperCase()).next();
                            //ADD Player Stats vertex and Player-Player Stats edge
                            final Vertex player_season_stats = g.addV(Node.SEASON)
                                    .property(Property.YEAR[0], season_year)
                                    .property(Property.ROLE[0], role).property(Property.PLAYED_MATCHES[0], played_matches)
                                    .property(Property.AVERAGE[0], avg_rating)
                                    .property(Property.FANTA_AVERAGE[0], avg_fantarating)
                                    .property(Property.GAUSS_AVERAGE[0], avg_gaussian_rating)
                                    .property(Property.GAUSS_FANTA_AVERAGE[0], avg_gaussian_fantarating)
                                    .property(Property.SCORED_GOALS[0], scored_goals)
                                    .property(Property.CONCEDED_GOALS[0], conceded_goals)
                                    .property(Property.ASSISTS[0], assists).property(Property.YELLOW_CARDS[0], yellow_cards)
                                    .property(Property.RED_CARDS[0], red_cards).property(Property.OWN_GOALS[0], own_goals).next();
                            g.V(player).as("a").V(player_season_stats).addE(Branch.PLAYER_TO_SEASON).from("a").next();
                            g.V(player_season_stats).as("a").V(season_team).addE(Branch.PLAYER_TO_SEASON).from("a")
                                    .next();
                        } else {
                            final Vertex player_season_stats = g.addV(Node.SEASON)
                                    .property(Property.YEAR[0], season_year)
                                    .property(Property.ROLE[0], role).property(Property.PLAYED_MATCHES[0], played_matches)
                                    .property(Property.AVERAGE[0], avg_rating)
                                    .property(Property.FANTA_AVERAGE[0], avg_fantarating)
                                    .property(Property.GAUSS_AVERAGE[0], avg_gaussian_rating)
                                    .property(Property.GAUSS_FANTA_AVERAGE[0], avg_gaussian_fantarating)
                                    .property(Property.SCORED_GOALS[0], scored_goals)
                                    .property(Property.CONCEDED_GOALS[0], conceded_goals)
                                    .property(Property.ASSISTS[0], assists).property(Property.YELLOW_CARDS[0], yellow_cards)
                                    .property(Property.RED_CARDS[0], red_cards).property(Property.OWN_GOALS[0], own_goals)
                                    .property(Property.TEAM_LABEL[0], playing_team.toUpperCase()).next();
                            g.V(player).as("a").V(player_season_stats).addE(Branch.PLAYER_TO_SEASON).from("a").next();
                        }
                    }
                }
                // Check and ADD Prosecutor vertex
                boolean exist = g.V().hasLabel(Node.PROSECUTOR).has(Property.NAME[0], prosecutor_name).hasNext();
                final Vertex prosecutor;
                if (!exist) {
                    prosecutor_counter = prosecutor_counter + 1;
                    prosecutor = g.addV(Node.PROSECUTOR).property(Property.NAME[0], prosecutor_name)
                            .property(Property.PROSECUTOR_ID[0], prosecutor_counter).next();
                } else {
                    prosecutor = g.V().hasLabel(Node.PROSECUTOR).has(Property.NAME[0], prosecutor_name).next();
                }
                // ADD Player-Prosecutor edge
                g.V(player).as("a").V(prosecutor).addE(Branch.PLAYER_TO_PROSECUTOR).from("a").next();
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
            Object obj = parser.parse(new FileReader(new PopulationBuilder()
                    .getFileFromResources("teams.txt")));
            long team_counter = 0;
            long president_counter = 0;
            long coach_counter = 0;
            long stadium_counter = 0;
            JSONObject jsonObject = (JSONObject) obj;
            for (Object key : jsonObject.keySet()) {
                //GET Team/Coach/President/Stadium from file
                //ADD these vertices and relative edges
                String name = (String) key;
                JSONObject team_info = (JSONObject) jsonObject.get(key);
                String logo = (String) team_info.get("logo");
                team_counter = team_counter + 1;
                final Vertex team = g.addV(Node.TEAM).property(Property.NAME[0], name)
                        .property(Property.LOGO[0], logo)
                        .property(Property.TEAM_ID[0], team_counter).next();
                //team vertex added to graph
                JSONObject stadium_info = (JSONObject) team_info.get("stadium");
                String stadium_name = (String) stadium_info.get("name");
                String stadium_place = (String) stadium_info.get("city");
                Long stadium_fans = (Long) stadium_info.get("capacity");
                String stadium_img = (String) stadium_info.get("img");
                boolean stadium_exist = g.V().hasLabel(Node.STADIUM).has(Property.NAME[0], stadium_name).hasNext();
                final Vertex stadium;
                if (!stadium_exist) {
                    stadium_counter = stadium_counter + 1;
                    stadium = g.addV(Node.STADIUM).property(Property.NAME[0], stadium_name)
                            .property(Property.CITY[0], stadium_place).property(Property.CAPACITY[0], stadium_fans)
                            .property(Property.IMG[0], stadium_img).property(Property.STADIUM_ID[0], stadium_counter).next();
                } else {
                    stadium = g.V().hasLabel(Node.STADIUM).has(Property.NAME[0], stadium_name).next();
                }
                boolean league_exist = g.V().hasLabel(Node.LEAGUE)
                        .has(Property.NAME[0], "Serie A TIM").hasNext();
                final Vertex league;
                if (!league_exist) {
                    league = g.addV(Node.LEAGUE).property(Property.NAME[0], "Serie A TIM")
                            .property(Property.COUNTRY[0], "ITALIA").next();
                } else {
                    league = g.V().hasLabel(Node.LEAGUE).has(Property.NAME[0], "Serie A TIM").next();
                }
                //stadium vertex added to graph
                JSONObject president_info = (JSONObject) team_info.get("president");
                String pres_name = (String) president_info.get("name");
                String pres_place = (String) president_info.get("birthplace");
                String pres_data = (String) president_info.get("birthdate");
                String pres_nat = (String) president_info.get("nationality");
                president_counter = president_counter + 1;
                final Vertex president = g.addV(Node.PRESIDENT).property(Property.NAME[0], pres_name)
                        .property(Property.BIRTHDATE[0], pres_data.split(" ")[0])
                        .property(Property.BIRTHPLACE[0], pres_place)
                        .property(Property.NATIONALITY[0], pres_nat).property(Property.PRESIDENT_ID[0], president_counter).next();
                //president vertex added to graph
                JSONObject coach_info = (JSONObject) team_info.get("coach");
                String coach_name = (String) coach_info.get("name");
                String coach_place = (String) coach_info.get("birthplace");
                String coach_data = (String) coach_info.get("birthdate");
                String coach_nat = (String) coach_info.get("nationality");
                Long coach_schema = (Long) coach_info.get("module");
                coach_counter = coach_counter + 1;
                final Vertex coach = g.addV(Node.COACH).property(Property.NAME[0], coach_name)
                        .property(Property.BIRTHDATE[0], coach_data.split(" ")[0])
                        .property(Property.BIRTHPLACE[0], coach_place)
                        .property(Property.NATIONALITY[0], coach_nat).property(Property.MODULE[0], coach_schema)
                        .property(Property.COACH_ID[0], coach_counter).next();
                //coach vertex added to graph
                g.V(team).as("a").V(stadium).addE(Branch.TEAM_TO_STADIUM).from("a").next();
                g.V(team).as("a").V(league).addE(Branch.TEAM_TO_LEAGUE).from("a").next();
                g.V(coach).as("a").V(team).addE(Branch.COACH_TO_TEAM).from("a").next();
                g.V(coach).as("a").V(president).addE(Branch.COACH_TO_PRESIDENT).from("a").next();
                g.V(president).as("a").V(team).addE(Branch.PRESIDENT_TO_TEAM).from("a").next();

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

    public void populateGraph(GraphTraversalSource g) {
        createInfosVertices(g);
        createPlayersVertices(g);
    }

}
