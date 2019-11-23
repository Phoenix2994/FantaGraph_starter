package graph.schema;

import org.janusgraph.core.*;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.janusgraph.core.schema.JanusGraphManagement;
import utilities.labels.Branch;
import utilities.labels.CompositeIndex;
import utilities.labels.Node;
import utilities.labels.Property;

public class SchemaBuilder {
    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaBuilder.class);

    /**
     * Creates the vertex labels.
     */
    private static void createVertexLabels(final JanusGraphManagement management) {
        for (String nodeLabel : Node.NODES) {
            management.makeVertexLabel(nodeLabel).make();
        }
    }

    /**
     * Creates the edge labels.
     */
    private static void createEdgeLabels(final JanusGraphManagement management) {
        for (String branchLabel : Branch.BRANCHES) {
            management.makeEdgeLabel(branchLabel).make();
        }
    }

    /**
     * Creates the properties for vertices, edges, and meta-properties.
     */
    private static void createProperties(final JanusGraphManagement management) {
        for (String[] propertyLabel : Property.PROPERTIES) {
            switch (propertyLabel[1]) {
                case "string":
                    management.makePropertyKey(propertyLabel[0]).dataType(String.class).make();
                    break;
                case "long":
                    management.makePropertyKey(propertyLabel[0]).dataType(Long.class).make();
                    break;
                case "double":
                    management.makePropertyKey(propertyLabel[0]).dataType(Double.class).make();
                    break;
            }
        }
    }

    public void dropPreviousGraph() throws Exception {
        JanusGraph graph_old = JanusGraphFactory.open("conf/janusgraph-cassandra-elasticsearch.properties");
        if (graph_old != null) {
            JanusGraphFactory.drop(graph_old);
        }
    }

    private static void buildCompositeIndex(JanusGraphManagement mgmt) {
        for( String index: CompositeIndex.INDICES){
            mgmt.buildIndex(index, Vertex.class).addKey(mgmt.getPropertyKey(index.toLowerCase()
                    .replace("by", " "))).unique().buildCompositeIndex();
        }
    }

    public void createSchema(final JanusGraphManagement management) {
        LOGGER.info("creating schema");
        createProperties(management);
        createVertexLabels(management);
        createEdgeLabels(management);
        buildCompositeIndex(management);
        management.commit();
    }


}

