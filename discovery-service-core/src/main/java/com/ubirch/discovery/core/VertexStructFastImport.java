package com.ubirch.discovery.core;

import com.tinkerpop.gremlin.java.GremlinPipeline;
import org.apache.tinkerpop.gremlin.process.traversal.Bindings;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.Map;

public class VertexStructFastImport {

    private Vertex vertex;
    private String id;
    private boolean exist = false;

    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass().getSimpleName());


    public VertexStructFastImport(String id, GraphTraversalSource g, HashMap<String, String> properties, Bindings b, String label) {
        this.id = id;
        addVertex(properties, g, b, label);
        if (this.vertex != null) this.exist = true;
    }

    public String getId() {
        return id;
    }


    public boolean exist() {
        return exist;
    }


    public void addVertex(HashMap<String, String> properties, GraphTraversalSource g, Bindings b, String label) {
        if (exist) {
            throw new InstantiationError("Vertex already exist in the database");
        }
        logger.info("Adding vetex with key= " + id );
        this.vertex = g.addV(b.of("label", label)).property("keyy", b.of("keyy", id)).next();

        for (Map.Entry<String, String> entry : properties.entrySet()) {
            GremlinPipeline pipe = new GremlinPipeline();
            pipe.start(g.V(this.vertex.id()).property(entry.getKey(), b.of(entry.getKey(), entry.getValue())).next());
        }

    }

    public Vertex getVertex() {
        return this.vertex;
    }
}
