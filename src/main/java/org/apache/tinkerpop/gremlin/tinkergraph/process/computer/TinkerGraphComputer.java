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
package org.apache.tinkerpop.gremlin.tinkergraph.process.computer;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.GraphFilter;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.traversal.strategy.optimization.GraphFilterStrategy;
import org.apache.tinkerpop.gremlin.process.computer.util.ComputerGraph;
import org.apache.tinkerpop.gremlin.process.computer.util.DefaultComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.util.GraphComputerHelper;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalInterruptedException;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerHelper;
import org.apache.tinkerpop.gremlin.util.function.TriConsumer;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.*;
import java.util.function.Consumer;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class TinkerGraphComputer implements GraphComputer {

    static {
        // GraphFilters are expensive w/ TinkerGraphComputer as everything is already in memory
        TraversalStrategies.GlobalCache.registerStrategies(TinkerGraphComputer.class,
                TraversalStrategies.GlobalCache.getStrategies(GraphComputer.class).clone().removeStrategies(GraphFilterStrategy.class));
    }

    private ResultGraph resultGraph = null;
    private Persist persist = null;

    private VertexProgram<?> vertexProgram;
    private final TinkerGraph graph;
    private TinkerMemory memory;
    private final TinkerMessageBoard messageBoard = new TinkerMessageBoard();
    private boolean executed = false;
    private final Set<MapReduce> mapReducers = new HashSet<>();
    private int workers = Runtime.getRuntime().availableProcessors();
    private final GraphFilter graphFilter = new GraphFilter();

    private final ThreadFactory threadFactoryBoss = new BasicThreadFactory.Builder().namingPattern(TinkerGraphComputer.class.getSimpleName() + "-boss").build();

    /**
     * An {@code ExecutorService} that schedules up background work. Since a {@link GraphComputer} is only used once
     * for a {@link VertexProgram} a single threaded executor is sufficient.
     */
    private final ExecutorService computerService = Executors.newSingleThreadExecutor(threadFactoryBoss);

    public TinkerGraphComputer(final TinkerGraph graph) {
        this.graph = graph;
    }

    @Override
    public GraphComputer result(final ResultGraph resultGraph) {
        this.resultGraph = resultGraph;
        return this;
    }

    @Override
    public GraphComputer persist(final Persist persist) {
        this.persist = persist;
        return this;
    }

    @Override
    public GraphComputer program(final VertexProgram vertexProgram) {
        this.vertexProgram = vertexProgram;
        return this;
    }

    @Override
    public GraphComputer mapReduce(final MapReduce mapReduce) {
        this.mapReducers.add(mapReduce);
        return this;
    }

    @Override
    public GraphComputer workers(final int workers) {
        this.workers = workers;
        return this;
    }

    @Override
    public GraphComputer vertices(final Traversal<Vertex, Vertex> vertexFilter) {
        this.graphFilter.setVertexFilter(vertexFilter);
        return this;
    }

    @Override
    public GraphComputer edges(final Traversal<Vertex, Edge> edgeFilter) {
        this.graphFilter.setEdgeFilter(edgeFilter);
        return this;
    }

    @Override
    public Future<ComputerResult> submit() {
        // a graph computer can only be executed once
        if (this.executed)
            throw Exceptions.computerHasAlreadyBeenSubmittedAVertexProgram();
        else
            this.executed = true;
        // it is not possible execute a computer if it has no vertex program nor mapreducers
        if (null == this.vertexProgram && this.mapReducers.isEmpty())
            throw GraphComputer.Exceptions.computerHasNoVertexProgramNorMapReducers();
        // it is possible to run mapreducers without a vertex program
        if (null != this.vertexProgram) {
            GraphComputerHelper.validateProgramOnComputer(this, this.vertexProgram);
            this.mapReducers.addAll(this.vertexProgram.getMapReducers());
        }
        // get the result graph and persist state to use for the computation
        this.resultGraph = GraphComputerHelper.getResultGraphState(Optional.ofNullable(this.vertexProgram), Optional.ofNullable(this.resultGraph));
        this.persist = GraphComputerHelper.getPersistState(Optional.ofNullable(this.vertexProgram), Optional.ofNullable(this.persist));
        if (!this.features().supportsResultGraphPersistCombination(this.resultGraph, this.persist))
            throw GraphComputer.Exceptions.resultGraphPersistCombinationNotSupported(this.resultGraph, this.persist);
        // ensure requested workers are not larger than supported workers
        if (this.workers > this.features().getMaxWorkers())
            throw GraphComputer.Exceptions.computerRequiresMoreWorkersThanSupported(this.workers, this.features().getMaxWorkers());

        // initialize the memory
        this.memory = new TinkerMemory(this.vertexProgram, this.mapReducers);
        return computerService.submit(new Callable<ComputerResult>() {
            @Override
            public ComputerResult call() throws Exception {
                final long time = System.currentTimeMillis();
                final TinkerGraphComputerView view = TinkerHelper.createGraphComputerView(TinkerGraphComputer.this.graph, TinkerGraphComputer.this.graphFilter, null != TinkerGraphComputer.this.vertexProgram ? TinkerGraphComputer.this.vertexProgram.getVertexComputeKeys() : Collections.emptySet());
                final TinkerWorkerPool workers = new TinkerWorkerPool(TinkerGraphComputer.this.graph, TinkerGraphComputer.this.memory, TinkerGraphComputer.this.workers);
                try {
                    if (null != TinkerGraphComputer.this.vertexProgram) {
                        // execute the vertex program
                        TinkerGraphComputer.this.vertexProgram.setup(TinkerGraphComputer.this.memory);
                        while (true) {
                            if (Thread.interrupted()) throw new TraversalInterruptedException();
                            TinkerGraphComputer.this.memory.completeSubRound();
                            workers.setVertexProgram(TinkerGraphComputer.this.vertexProgram);
                            workers.executeVertexProgram(new TriConsumer<Iterator<Vertex>, VertexProgram, TinkerWorkerMemory>() {
                                @Override
                                public void accept(Iterator<Vertex> vertices, VertexProgram vertexProgram, TinkerWorkerMemory workerMemory) {
                                    vertexProgram.workerIterationStart(workerMemory.asImmutable());
                                    while (vertices.hasNext()) {
                                        final Vertex vertex = vertices.next();
                                        if (Thread.interrupted()) throw new TraversalInterruptedException();
                                        vertexProgram.execute(
                                                ComputerGraph.vertexProgram(vertex, vertexProgram),
                                                new TinkerMessenger<>(vertex, TinkerGraphComputer.this.messageBoard, vertexProgram.getMessageCombiner()),
                                                workerMemory);
                                    }
                                    vertexProgram.workerIterationEnd(workerMemory.asImmutable());
                                    workerMemory.complete();
                                }
                            });
                            TinkerGraphComputer.this.messageBoard.completeIteration();
                            TinkerGraphComputer.this.memory.completeSubRound();
                            if (TinkerGraphComputer.this.vertexProgram.terminate(TinkerGraphComputer.this.memory)) {
                                TinkerGraphComputer.this.memory.incrIteration();
                                break;
                            } else {
                                TinkerGraphComputer.this.memory.incrIteration();
                            }
                        }
                        view.complete(); // drop all transient vertex compute keys
                    }

                    // execute mapreduce jobs
                    for (final MapReduce mapReduce : mapReducers) {
                        final TinkerMapEmitter<?, ?> mapEmitter = new TinkerMapEmitter<>(mapReduce.doStage(MapReduce.Stage.REDUCE));
                        final SynchronizedIterator<Vertex> vertices = new SynchronizedIterator<>(TinkerGraphComputer.this.graph.vertices());
                        workers.setMapReduce(mapReduce);
                        workers.executeMapReduce(new Consumer<MapReduce>() {
                            @Override
                            public void accept(MapReduce workerMapReduce) {
                                workerMapReduce.workerStart(MapReduce.Stage.MAP);
                                while (true) {
                                    if (Thread.interrupted()) throw new TraversalInterruptedException();
                                    final Vertex vertex = vertices.next();
                                    if (null == vertex) break;
                                    workerMapReduce.map(ComputerGraph.mapReduce(vertex), mapEmitter);
                                }
                                workerMapReduce.workerEnd(MapReduce.Stage.MAP);
                            }
                        });
                        // sort results if a map output sort is defined
                        mapEmitter.complete(mapReduce);

                        // no need to run combiners as this is single machine
                        if (mapReduce.doStage(MapReduce.Stage.REDUCE)) {
                            final TinkerReduceEmitter<?, ?> reduceEmitter = new TinkerReduceEmitter<>();
                            final SynchronizedIterator<Map.Entry<?, Queue<?>>> keyValues = new SynchronizedIterator((Iterator) mapEmitter.reduceMap.entrySet().iterator());
                            workers.executeMapReduce(new Consumer<MapReduce>() {
                                @Override
                                public void accept(MapReduce workerMapReduce) {
                                    workerMapReduce.workerStart(MapReduce.Stage.REDUCE);
                                    while (true) {
                                        if (Thread.interrupted()) throw new TraversalInterruptedException();
                                        final Map.Entry<?, Queue<?>> entry = keyValues.next();
                                        if (null == entry) break;
                                        workerMapReduce.reduce(entry.getKey(), entry.getValue().iterator(), reduceEmitter);
                                    }
                                    workerMapReduce.workerEnd(MapReduce.Stage.REDUCE);
                                }
                            });
                            reduceEmitter.complete(mapReduce); // sort results if a reduce output sort is defined
                            mapReduce.addResultToMemory(TinkerGraphComputer.this.memory, reduceEmitter.reduceQueue.iterator());
                        } else {
                            mapReduce.addResultToMemory(TinkerGraphComputer.this.memory, mapEmitter.mapQueue.iterator());
                        }
                    }
                    // update runtime and return the newly computed graph
                    TinkerGraphComputer.this.memory.setRuntime(System.currentTimeMillis() - time);
                    TinkerGraphComputer.this.memory.complete(); // drop all transient properties and set iteration
                    // determine the resultant graph based on the result graph/persist state
                    final Graph resultGraph = view.processResultGraphPersist(TinkerGraphComputer.this.resultGraph, TinkerGraphComputer.this.persist);
                    TinkerHelper.dropGraphComputerView(TinkerGraphComputer.this.graph); // drop the view from the original source graph
                    return new DefaultComputerResult(resultGraph, TinkerGraphComputer.this.memory.asImmutable());
                } catch (InterruptedException ie) {
                    workers.closeNow();
                    throw new TraversalInterruptedException();
                } catch (Exception ex) {
                    workers.closeNow();
                    throw new RuntimeException(ex);
                } finally {
                    workers.close();
                }
            }
        });
    }

    @Override
    public String toString() {
        return StringFactory.graphComputerString(this);
    }

    private static class SynchronizedIterator<V> {

        private final Iterator<V> iterator;

        public SynchronizedIterator(final Iterator<V> iterator) {
            this.iterator = iterator;
        }

        public synchronized V next() {
            return this.iterator.hasNext() ? this.iterator.next() : null;
        }
    }

    @Override
    public Features features() {
        return new Features() {

            @Override
            public int getMaxWorkers() {
                return Runtime.getRuntime().availableProcessors();
            }

            @Override
            public boolean supportsVertexAddition() {
                return false;
            }

            @Override
            public boolean supportsVertexRemoval() {
                return false;
            }

            @Override
            public boolean supportsVertexPropertyRemoval() {
                return false;
            }

            @Override
            public boolean supportsEdgeAddition() {
                return false;
            }

            @Override
            public boolean supportsEdgeRemoval() {
                return false;
            }

            @Override
            public boolean supportsEdgePropertyAddition() {
                return false;
            }

            @Override
            public boolean supportsEdgePropertyRemoval() {
                return false;
            }
        };
    }
}