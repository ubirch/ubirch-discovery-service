package com.ubirch.discovery.core;

import com.tinkerpop.gremlin.java.GremlinPipeline;
import org.apache.tinkerpop.gremlin.process.traversal.Bindings;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

public class FastImport {

    private static final String LABEL = "label";
    private static final String ID = "keyy";
    private static final String OUT_V = "outV";
    private static final String IN_V = "inV";
    private static final ArrayList<String> LIST_NAME_BLOCKCHAIN = new ArrayList<>(
            Arrays.asList("blockchain_tx_id", "ETHEREUM_TESTNET_RINKEBY_TESTNET_NETWORK")
    );
    private static final ArrayList<String> LIST_NAME_TREE = new ArrayList<>(
            Arrays.asList("slave-tree-id")
    );
    private static final ArrayList<String> LIST_NAME_DEVICEID = new ArrayList<>(
            Arrays.asList("device-id")
    );
    private static String[] libeles = {"value", "category", "name", "key"};
    private static int VALUE = 0;
    private static int CATEGORY = 1;
    private static int NAME = 2;
    private static int KEY = 3;

    private static String[] data;


    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass().getSimpleName());
    private static HashMap<String, VertexStructFastImport> listV = new HashMap<>();
    private static GraphTraversalSource g;
    private static Bindings b;

    public static void main(String[] args) {
        String csvFile = args[0];
        String csvSplitBy;
        if (args.length == 1) {
            csvSplitBy = ";";
            logger.info("using default split command: \";\"");
        } else {
            csvSplitBy = args[1];
        }
        fastImport(csvFile, csvSplitBy);
    }

    public static void fastImport(String csvFile, String csvSplitBy) {

        BufferedReader br = null;
        String line;

        try {

            GremlinServerConnector sCon = new GremlinServerConnector();
            Graph graph = sCon.getGraph();
            g = sCon.getTraversal();
            b = sCon.getBindings();

            // clean DB
            g.V().drop().iterate();


            br = new BufferedReader(new FileReader(csvFile));
            int i = 1;
            br.readLine();
            while ((line = br.readLine()) != null) {

                // use comma as separator
                data = line.split(csvSplitBy);

                System.out.println("Data [" + libeles[0] + "= " + data[0] +
                        " , " + libeles[1] + "=" + data[1] + " , " + libeles[2] +
                        "=" + data[2] + " , " + libeles[3] + "=" + data[3] + "]");
                HashMap<String, String> pTo = new HashMap<>();
                HashMap<String, String> pFrom = new HashMap<>();
                HashMap<String, String> e = new HashMap<>();

                e.put("category", data[CATEGORY]);
                e.put("name", data[NAME]);


                String id1 = data[KEY];
                String id2 = data[VALUE];

                switch (howMany(id1, id2)) {
                    case 0: {
                        case0(id1, pTo, id2, pFrom, e);
                    }
                    case 1: {
                        case1(id1, pFrom, id2, pTo, e);
                        break;
                    }
                    case 2: {
                        case2(id1, id2, e);
                        break;
                    }
                }

                logger.info("i");
                i++;
            }
            sCon.closeConnection();

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * If no vertex have been initialised
     *
     * @param id1   Id of the first vertex
     * @param pTo   Map containing the properties of the first vertex.
     * @param id2   Id of the second vertex.
     * @param pFrom Map containing the properties of the second vertex.
     * @param e     Map contining the properties of the edge.
     */
    private static void case0(String id1, HashMap<String, String> pTo, String id2, HashMap<String, String> pFrom, HashMap<String, String> e) {
        logger.info("case0");
        VertexStructFastImport v1 = new VertexStructFastImport(id1, g, pTo, b, getLabelTo());
        listV.put(id1, v1);
        VertexStructFastImport v2 = new VertexStructFastImport(id2, g, pFrom, b, getLabelFrom());
        listV.put(id2, v2);
        createEdge(v1, v2, e);
    }

    /**
     * If only one vertex has been initialised.
     *
     * @param id1   Id of the first vertex.
     * @param pTo   Map containing the properties of the first vertex.
     * @param id2   Id of the second vertex.
     * @param pFrom Map containing the properties of the second vertex.
     * @param e     HashMap containing the properties of the edge.
     */
    private static void case1(String id1, HashMap<String, String> pTo, String id2, HashMap<String, String> pFrom, HashMap<String, String> e) {
        if (listV.containsKey(id1)) { //first one
            VertexStructFastImport v2 = new VertexStructFastImport(id2, g, pFrom, b, getLabelFrom());
            listV.put(id2, v2);
            createEdge(listV.get(id1), v2, e);
        } else {
            VertexStructFastImport v1 = new VertexStructFastImport(id1, g, pTo, b, getLabelTo());
            listV.put(id1, v1);
            createEdge(v1, listV.get(id2), e);
        }
    }

    /**
     * If the two vertices have already been initialised.
     *
     * @param id1 Id of the first vertex.
     * @param id2 Id of the second vertex.
     * @param e   HashMap containing the properties of the edge.
     */
    private static void case2(String id1, String id2, HashMap<String, String> e) {
        logger.info("case2");
        createEdge(listV.get(id1), listV.get(id2), e);
    }


    /**
     * Determine the label of one vertex depending on its link.
     *
     * @return type of label.
     */
    private static String getLabelFrom() {
        if (LIST_NAME_BLOCKCHAIN.contains(data[NAME])) {
            return "tree";
        }
        return "generic";
    }

    /**
     * Determine the label of a vertex depending on its link.
     *
     * @return type of label.
     */
    private static String getLabelTo() {
        HashMap<String, String> pTo = new HashMap<>();
        pTo.put("keyy", data[KEY]);
        if (LIST_NAME_BLOCKCHAIN.contains(data[NAME])) {
            return "blockchain";
        } else if (LIST_NAME_DEVICEID.contains(data[NAME])) {
            return "device-id";
        } else if (LIST_NAME_TREE.contains(data[NAME])) {
            return "tree";
        }
        return "generic";
    }

    /**
     * Determine how many vertices have already been initialised.
     *
     * @param id1 The id of the first vertex.
     * @param id2 The id of the second vertex.
     * @return An int (0, 1, 2) representing how many vertices have been initialised.
     */
    private static int howMany(String id1, String id2) {
        int count = 0;
        if (listV.containsKey(id1)) count++;
        if (listV.containsKey(id2)) count++;
        return count;
    }

    /**
     * Create an edge between two vertices.
     *
     * @param vertexFrom Vertex where the edge will go.
     * @param vertexTo   Vertex from where the edge starts.
     * @param properties Map containing the properties of the edge.
     */
    protected static void createEdge(VertexStructFastImport vertexFrom, VertexStructFastImport vertexTo, HashMap<String, String> properties) {
        g.V(b.of(OUT_V, vertexFrom.getVertex())).as("a").V(b.of(IN_V, vertexTo.getVertex())).addE(b.of(LABEL, properties.get("name"))).from("a").iterate(); // add the label // TODO: modify label name

        // add the other properties
        for (HashMap.Entry<String, String> entry : properties.entrySet()) {
            GremlinPipeline pipe = new GremlinPipeline();
            pipe.start(g.V(vertexFrom.getVertex()).outE().as("e").inV().has(ID, vertexTo.getId()).select("e").property(entry.getKey(), b.of(entry.getKey(), entry.getValue())).next());
        }
    }

}

























