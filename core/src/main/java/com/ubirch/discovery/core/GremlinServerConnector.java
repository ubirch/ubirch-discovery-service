package com.ubirch.discovery.core;

import org.apache.tinkerpop.gremlin.process.traversal.Bindings;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;

class GremlinServerConnector {

    private GraphTraversalSource g;
    private Graph graph;
    private Bindings b;

    private final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass().getSimpleName());


    GremlinServerConnector() {
        graph = EmptyGraph.instance();
        g = getTraversal(graph);
        b = Bindings.instance();
    }

    // initialisation and connection to server

    void closeConnection() {
        try {
            g.close();
        } catch (Exception e) {
            logger.error("", e);
        }
        graph = null;
        g = null;
        b = null;
    }

    private GraphTraversalSource getTraversal(Graph graph) {
        GraphTraversalSource g = null;
        try {
            g = graph.traversal().withRemote("core/src/main/ressources/remote-graph.properties");
        } catch (Exception e) {
            logger.error("", e);
        }
        return g;
    }

    Graph getGraph() {
        return this.graph;
    }

    GraphTraversalSource getTraversal() {
        return this.g;
    }

    Bindings getBindings() {
        return b;
    }
}
