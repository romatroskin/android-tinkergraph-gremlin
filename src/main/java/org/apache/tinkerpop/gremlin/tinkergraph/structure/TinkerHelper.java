/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.tinkergraph.structure;

import org.apache.tinkerpop.gremlin.process.computer.GraphFilter;
import org.apache.tinkerpop.gremlin.process.computer.VertexComputeKey;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.tinkergraph.process.computer.TinkerGraphComputerView;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class TinkerHelper {

    private TinkerHelper() {
    }

    protected static Edge addEdge(final TinkerGraph graph, final TinkerVertex outVertex, final TinkerVertex inVertex, final String label, final Object... keyValues) {
        ElementHelper.validateLabel(label);
        ElementHelper.legalPropertyKeyValueArray(keyValues);

        Object idValue = graph.edgeIdManager.convert(ElementHelper.getIdValue(keyValues).orElse(null));

        final Edge edge;
        if (null != idValue) {
            if (graph.edges.containsKey(idValue))
                throw Graph.Exceptions.edgeWithIdAlreadyExists(idValue);
        } else {
            idValue = graph.edgeIdManager.getNextId(graph);
        }

        edge = new TinkerEdge(idValue, outVertex, label, inVertex);
        ElementHelper.attachProperties(edge, keyValues);
        graph.edges.put(edge.id(), edge);
        TinkerHelper.addOutEdge(outVertex, label, edge);
        TinkerHelper.addInEdge(inVertex, label, edge);
        return edge;

    }

    protected static void addOutEdge(final TinkerVertex vertex, final String label, final Edge edge) {
        if (null == vertex.outEdges) vertex.outEdges = new HashMap<>();
        Set<Edge> edges = vertex.outEdges.get(label);
        if (null == edges) {
            edges = new HashSet<>();
            vertex.outEdges.put(label, edges);
        }
        edges.add(edge);
    }

    protected static void addInEdge(final TinkerVertex vertex, final String label, final Edge edge) {
        if (null == vertex.inEdges) vertex.inEdges = new HashMap<>();
        Set<Edge> edges = vertex.inEdges.get(label);
        if (null == edges) {
            edges = new HashSet<>();
            vertex.inEdges.put(label, edges);
        }
        edges.add(edge);
    }

    public static List<TinkerVertex> queryVertexIndex(final TinkerGraph graph, final String key, final Object value) {
        return null == graph.vertexIndex ? Collections.emptyList() : graph.vertexIndex.get(key, value);
    }

    public static List<TinkerEdge> queryEdgeIndex(final TinkerGraph graph, final String key, final Object value) {
        return null == graph.edgeIndex ? Collections.emptyList() : graph.edgeIndex.get(key, value);
    }

    public static boolean inComputerMode(final TinkerGraph graph) {
        return null != graph.graphComputerView;
    }

    public static TinkerGraphComputerView createGraphComputerView(final TinkerGraph graph, final GraphFilter graphFilter, final Set<VertexComputeKey> computeKeys) {
        return graph.graphComputerView = new TinkerGraphComputerView(graph, graphFilter, computeKeys);
    }

    public static TinkerGraphComputerView getGraphComputerView(final TinkerGraph graph) {
        return graph.graphComputerView;
    }

    public static void dropGraphComputerView(final TinkerGraph graph) {
        graph.graphComputerView = null;
    }

    public static Map<String, List<VertexProperty>> getProperties(final TinkerVertex vertex) {
        return null == vertex.properties ? Collections.emptyMap() : vertex.properties;
    }

    public static void autoUpdateIndex(final TinkerEdge edge, final String key, final Object newValue, final Object oldValue) {
        final TinkerGraph graph = (TinkerGraph) edge.graph();
        if (graph.edgeIndex != null)
            graph.edgeIndex.autoUpdate(key, newValue, oldValue, edge);
    }

    public static void autoUpdateIndex(final TinkerVertex vertex, final String key, final Object newValue, final Object oldValue) {
        final TinkerGraph graph = (TinkerGraph) vertex.graph();
        if (graph.vertexIndex != null)
            graph.vertexIndex.autoUpdate(key, newValue, oldValue, vertex);
    }

    public static void removeElementIndex(final TinkerVertex vertex) {
        final TinkerGraph graph = (TinkerGraph) vertex.graph();
        if (graph.vertexIndex != null)
            graph.vertexIndex.removeElement(vertex);
    }

    public static void removeElementIndex(final TinkerEdge edge) {
        final TinkerGraph graph = (TinkerGraph) edge.graph();
        if (graph.edgeIndex != null)
            graph.edgeIndex.removeElement(edge);
    }

    public static void removeIndex(final TinkerVertex vertex, final String key, final Object value) {
        final TinkerGraph graph = (TinkerGraph) vertex.graph();
        if (graph.vertexIndex != null)
            graph.vertexIndex.remove(key, value, vertex);
    }

    public static void removeIndex(final TinkerEdge edge, final String key, final Object value) {
        final TinkerGraph graph = (TinkerGraph) edge.graph();
        if (graph.edgeIndex != null)
            graph.edgeIndex.remove(key, value, edge);
    }

    public static Iterator<TinkerEdge> getEdges(final TinkerVertex vertex, final Direction direction, final String... edgeLabels) {
        final List<Edge> edges = new ArrayList<>();
        if (direction.equals(Direction.OUT) || direction.equals(Direction.BOTH)) {
            if (vertex.outEdges != null) {
                if (edgeLabels.length == 0)
                    vertex.outEdges.values().forEach(new Consumer<Set<Edge>>() {
                        @Override
                        public void accept(Set<Edge> c) {
                            edges.addAll(c);
                        }
                    });
                else if (edgeLabels.length == 1)
                    edges.addAll(vertex.outEdges.getOrDefault(edgeLabels[0], Collections.emptySet()));
                else
                    Stream.of(edgeLabels).map(new Function<String, Set<Edge>>() {
                        @Override
                        public Set<Edge> apply(String key) {
                            return vertex.outEdges.get(key);
                        }
                    }).filter(new Predicate<Set<Edge>>() {
                        @Override
                        public boolean test(Set<Edge> obj) {
                            return Objects.nonNull(obj);
                        }
                    }).forEach(new Consumer<Set<Edge>>() {
                        @Override
                        public void accept(Set<Edge> c) {
                            edges.addAll(c);
                        }
                    });
            }
        }
        if (direction.equals(Direction.IN) || direction.equals(Direction.BOTH)) {
            if (vertex.inEdges != null) {
                if (edgeLabels.length == 0)
                    vertex.inEdges.values().forEach(new Consumer<Set<Edge>>() {
                        @Override
                        public void accept(Set<Edge> c) {
                            edges.addAll(c);
                        }
                    });
                else if (edgeLabels.length == 1)
                    edges.addAll(vertex.inEdges.getOrDefault(edgeLabels[0], Collections.emptySet()));
                else
                    Stream.of(edgeLabels).map(new Function<String, Set<Edge>>() {
                        @Override
                        public Set<Edge> apply(String key) {
                            return vertex.inEdges.get(key);
                        }
                    }).filter(new Predicate<Set<Edge>>() {
                        @Override
                        public boolean test(Set<Edge> obj) {
                            return Objects.nonNull(obj);
                        }
                    }).forEach(new Consumer<Set<Edge>>() {
                        @Override
                        public void accept(Set<Edge> c) {
                            edges.addAll(c);
                        }
                    });
            }
        }
        return (Iterator) edges.iterator();
    }

    public static Iterator<TinkerVertex> getVertices(final TinkerVertex vertex, final Direction direction, final String... edgeLabels) {
        final List<Vertex> vertices = new ArrayList<>();
        if (direction.equals(Direction.OUT) || direction.equals(Direction.BOTH)) {
            if (vertex.outEdges != null) {
                if (edgeLabels.length == 0)
                    vertex.outEdges.values().forEach(new Consumer<Set<Edge>>() {
                        @Override
                        public void accept(Set<Edge> set) {
                            set.forEach(new Consumer<Edge>() {
                                @Override
                                public void accept(Edge edge) {
                                    vertices.add(((TinkerEdge) edge).inVertex);
                                }
                            });
                        }
                    });
                else if (edgeLabels.length == 1)
                    vertex.outEdges.getOrDefault(edgeLabels[0], Collections.emptySet()).forEach(new Consumer<Edge>() {
                        @Override
                        public void accept(Edge edge) {
                            vertices.add(((TinkerEdge) edge).inVertex);
                        }
                    });
                else
                    Stream.of(edgeLabels).map(new Function<String, Set<Edge>>() {
                        @Override
                        public Set<Edge> apply(String key) {
                            return vertex.outEdges.get(key);
                        }
                    }).filter(new Predicate<Set<Edge>>() {
                        @Override
                        public boolean test(Set<Edge> obj) {
                            return Objects.nonNull(obj);
                        }
                    }).flatMap(new Function<Set<Edge>, Stream<? extends Edge>>() {
                        @Override
                        public Stream<? extends Edge> apply(Set<Edge> edges) {
                            return edges.stream();
                        }
                    }).forEach(new Consumer<Edge>() {
                        @Override
                        public void accept(Edge edge) {
                            vertices.add(((TinkerEdge) edge).inVertex);
                        }
                    });
            }
        }
        if (direction.equals(Direction.IN) || direction.equals(Direction.BOTH)) {
            if (vertex.inEdges != null) {
                if (edgeLabels.length == 0)
                    vertex.inEdges.values().forEach(new Consumer<Set<Edge>>() {
                        @Override
                        public void accept(Set<Edge> set) {
                            set.forEach(new Consumer<Edge>() {
                                @Override
                                public void accept(Edge edge) {
                                    vertices.add(((TinkerEdge) edge).outVertex);
                                }
                            });
                        }
                    });
                else if (edgeLabels.length == 1)
                    vertex.inEdges.getOrDefault(edgeLabels[0], Collections.emptySet()).forEach(new Consumer<Edge>() {
                        @Override
                        public void accept(Edge edge) {
                            vertices.add(((TinkerEdge) edge).outVertex);
                        }
                    });
                else
                    Stream.of(edgeLabels).map(new Function<String, Set<Edge>>() {
                        @Override
                        public Set<Edge> apply(String key) {
                            return vertex.inEdges.get(key);
                        }
                    }).filter(new Predicate<Set<Edge>>() {
                        @Override
                        public boolean test(Set<Edge> obj) {
                            return Objects.nonNull(obj);
                        }
                    }).flatMap(new Function<Set<Edge>, Stream<? extends Edge>>() {
                        @Override
                        public Stream<? extends Edge> apply(Set<Edge> edges) {
                            return edges.stream();
                        }
                    }).forEach(new Consumer<Edge>() {
                        @Override
                        public void accept(Edge edge) {
                            vertices.add(((TinkerEdge) edge).outVertex);
                        }
                    });
            }
        }
        return (Iterator) vertices.iterator();
    }

    public static Map<Object, Vertex> getVertices(final TinkerGraph graph) {
        return graph.vertices;
    }

    public static Map<Object, Edge> getEdges(final TinkerGraph graph) {
        return graph.edges;
    }
}
