/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */
package org.hibernate.ogm.datastore.neo4j.impl;

import static org.hibernate.ogm.datastore.neo4j.Environment.DEFAULT_NEO4J_ASSOCIATION_INDEX_NAME;
import static org.hibernate.ogm.datastore.neo4j.Environment.DEFAULT_NEO4J_ENTITY_INDEX_NAME;
import static org.hibernate.ogm.datastore.neo4j.Environment.DEFAULT_NEO4J_SEQUENCE_INDEX_NAME;
import static org.hibernate.ogm.datastore.neo4j.Environment.NEO4J_ASSOCIATION_INDEX_NAME;
import static org.hibernate.ogm.datastore.neo4j.Environment.NEO4J_ENTITY_INDEX_NAME;
import static org.hibernate.ogm.datastore.neo4j.Environment.NEO4J_SEQUENCE_INDEX_NAME;

import java.util.Map;
import java.util.Properties;

import org.hibernate.ogm.datastore.neo4j.spi.GraphDatabaseServiceFactory;
import org.hibernate.ogm.datastore.spi.DatastoreProvider;
import org.hibernate.ogm.dialect.GridDialect;
import org.hibernate.ogm.dialect.neo4j.Neo4jDialect;
import org.hibernate.ogm.grid.RowKey;
import org.hibernate.ogm.options.spi.MappingFactory;
import org.hibernate.ogm.service.impl.LuceneBasedQueryParserService;
import org.hibernate.ogm.service.impl.QueryParserService;
import org.hibernate.service.spi.Configurable;
import org.hibernate.service.spi.Startable;
import org.hibernate.service.spi.Stoppable;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.index.Index;

/**
 * Provides access to the Neo4j system.
 *
 * @author Davide D'Alto
 */
public class Neo4jDatastoreProvider implements DatastoreProvider, Startable, Stoppable, Configurable {

	private String sequenceIndexName = DEFAULT_NEO4J_SEQUENCE_INDEX_NAME;

	private String nodeIndexName = DEFAULT_NEO4J_ENTITY_INDEX_NAME;

	private String relationshipIndexName = DEFAULT_NEO4J_ASSOCIATION_INDEX_NAME;

	private GraphDatabaseService neo4jDb;

	private Neo4jSequenceGenerator neo4jSequenceGenerator;

	private GraphDatabaseServiceFactory graphDbFactory;

	@Override
	public Class<? extends QueryParserService> getDefaultQueryParserServiceType() {
		return LuceneBasedQueryParserService.class;
	}

	@Override
	public void configure(Map cfg) {
		graphDbFactory = new Neo4jGraphDatabaseServiceFactoryProvider().load( properties( cfg ) );
		sequenceIndexName = defaultIfNull( cfg, NEO4J_SEQUENCE_INDEX_NAME, DEFAULT_NEO4J_SEQUENCE_INDEX_NAME );
		nodeIndexName = defaultIfNull( cfg, NEO4J_ENTITY_INDEX_NAME, DEFAULT_NEO4J_ENTITY_INDEX_NAME );
		relationshipIndexName = defaultIfNull( cfg, NEO4J_ASSOCIATION_INDEX_NAME, DEFAULT_NEO4J_ASSOCIATION_INDEX_NAME );
	}

	private String defaultIfNull(Map<?, ?> cfg, String key, String defaultValue) {
		String indexName = (String) cfg.get( key );
		return indexName == null ? defaultValue : indexName;
	}

	@Override
	public void stop() {
		neo4jDb.shutdown();
	}

	@Override
	public void start() {
		this.neo4jDb = graphDbFactory.create();
		this.neo4jSequenceGenerator = new Neo4jSequenceGenerator( neo4jDb, sequenceIndexName );
		this.graphDbFactory = null;
	}

	private Properties properties(Map configuration) {
		Properties properties = new Properties();
		properties.putAll( configuration );
		return properties;
	}

	@Override
	public Class<? extends GridDialect> getDefaultDialect() {
		return Neo4jDialect.class;
	}

	public Node createNode() {
		return neo4jDb.createNode();
	}

	public GraphDatabaseService getDataBase() {
		return neo4jDb;
	}

	public int nextValue(RowKey key, int increment, int initialValue) {
		return neo4jSequenceGenerator.nextValue( key, increment, initialValue );
	}

	public Index<Node> getNodesIndex() {
		return neo4jDb.index().forNodes( nodeIndexName );
	}

	public Index<Relationship> getRelationshipsIndex() {
		return neo4jDb.index().forRelationships( relationshipIndexName );
	}

	@Override
	public Class<? extends MappingFactory<?>> getConfigurationBuilder() {
		return null;
	}

}
