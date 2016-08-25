/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.neo4j.bolt.dialect.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.hibernate.boot.model.relational.Sequence;
import org.hibernate.internal.util.collections.BoundedConcurrentHashMap;
import org.hibernate.ogm.datastore.neo4j.dialect.impl.BaseNeo4jSequenceGenerator;
import org.hibernate.ogm.datastore.neo4j.dialect.impl.NodeLabel;
import org.hibernate.ogm.datastore.neo4j.logging.impl.Log;
import org.hibernate.ogm.datastore.neo4j.logging.impl.LoggerFactory;
import org.hibernate.ogm.dialect.spi.NextValueRequest;
import org.hibernate.ogm.id.impl.OgmSequenceGenerator;
import org.hibernate.ogm.id.impl.OgmTableGenerator;
import org.hibernate.ogm.model.key.spi.IdSourceKey;
import org.hibernate.ogm.model.key.spi.IdSourceKeyMetadata;
import org.hibernate.ogm.model.key.spi.IdSourceKeyMetadata.IdSourceType;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.util.Resource;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.Label;

/**
 * Generates the next value of an id sequence as represented by {@link IdSourceKey}.
 * <p>
 * Both, {@link IdSourceType#TABLE} and {@link IdSourceType#SEQUENCE} are supported. For the table strategy, nodes in
 * the following form are used (the exact property names and the label value can be configured using the options exposed
 * by {@link OgmTableGenerator}):
 *
 * <pre>
 * (:hibernate_sequences:TABLE_BASED_SEQUENCE { sequence_name = 'ExampleSequence', current_value : 3 })
 * </pre>
 *
 * For the sequence strategy, nodes in the following form are used (the sequence name can be configured using the option
 * exposed by {@link OgmSequenceGenerator}):
 *
 * <pre>
 * (:SEQUENCE { sequence_name = 'ExampleSequence', next_val : 3 })
 * </pre>
 *
 * Sequences are created at startup.
 * <p>
 * A write lock is acquired on the node every time the sequence needs to be updated.
 *
 * @author Davide D'Alto &lt;davide@hibernate.org&gt;
 * @author Gunnar Morling
 */
public class BoltNeo4jSequenceGenerator extends BaseNeo4jSequenceGenerator {

	/**
	 * Query for creating SEQUENCE nodes.
	 */
	protected static final String SEQUENCE_CREATION_QUERY =
			"MERGE (n:" + NodeLabel.SEQUENCE.name() + " {" + SEQUENCE_NAME_PROPERTY + ": {sequenceName}} )"
			+ " RETURN n";

	protected static final String SEQUENCE_LOCK_QUERY =
			"MATCH (n:" + NodeLabel.SEQUENCE.name() + ")"
			+ " WHERE n." + SEQUENCE_NAME_PROPERTY + " = {sequenceName} "
			+ " SET n.__locked = true "
			+ " RETURN n";

	/**
	 * Query for retrieving the next value from SEQUENCE nodes.
	 */
	private static final String SEQUENCE_VALUE_QUERY =
			"MATCH (n:" + NodeLabel.SEQUENCE.name() + ")"
			+ " WHERE n." + SEQUENCE_NAME_PROPERTY + " = {sequenceName} "
			+ " REMOVE n.__locked "
			+ " SET n." + SEQUENCE_VALUE_PROPERTY + " = coalesce(n." + SEQUENCE_VALUE_PROPERTY + ", {initialValue}) + {increment}"
			+ " RETURN n." + SEQUENCE_VALUE_PROPERTY;

	private static final Log logger = LoggerFactory.getLogger();

	private final BoundedConcurrentHashMap<String, List<Statement>> queryCache;

	private final Driver driver;

	public BoltNeo4jSequenceGenerator(Driver driver, int sequenceCacheMaxSize) {
		this.driver = driver;
		this.queryCache = new BoundedConcurrentHashMap<String, List<Statement>>( sequenceCacheMaxSize, 20, BoundedConcurrentHashMap.Eviction.LIRS );
	}

	/**
	 * Create the sequence nodes setting the initial value if the node does not exist already.
	 * <p>
	 * All nodes are created inside the same transaction.
	 */
	public void createSequencesConstraints(List<Statement> statements, Iterable<Sequence> sequences) {
		addUniqueConstraintForSequences( statements );
	}

	public void createSequences(List<Statement> statements, Iterable<Sequence> sequences) {
		addSequences( statements, sequences );
	}

	private void addUniqueConstraintForSequences(List<Statement> statements) {
		Statement statement = createUniqueConstraintStatement( SEQUENCE_NAME_PROPERTY, NodeLabel.SEQUENCE.name() );
		statements.add( statement );
	}

	/**
	 * Adds a unique constraint to make sure that each node of the same "sequence table" is unique.
	 */
	private void addUniqueConstraintForTableBasedSequence(List<Statement> statements, IdSourceKeyMetadata generatorKeyMetadata) {
		Statement statement = createUniqueConstraintStatement( generatorKeyMetadata.getKeyColumnName(), generatorKeyMetadata.getName() );
		statements.add( statement );
	}

	private Statement createUniqueConstraintStatement(String propertyName, String label) {
		String queryString = createUniqueConstraintQuery( propertyName, label );
		Statement statement = new Statement( queryString );
		return statement;
	}

	/**
	 * Adds a node for each generator of type {@link IdSourceType#SEQUENCE}. Table-based generators are created lazily
	 * at runtime.
	 *
	 * @param sequences the generators to process
	 */
	private void addSequences(List<Statement> statements, Iterable<Sequence> sequences) {
		for ( Sequence sequence : sequences ) {
			addSequence( statements, sequence );
		}
	}

	private void addSequence(List<Statement> statements, Sequence sequence) {
		Statement statement = new Statement( SEQUENCE_CREATION_QUERY, Collections.<String, Object>singletonMap( SEQUENCE_NAME_QUERY_PARAM, sequence.getName().render() ) );
		statements.add( statement );
	}

	protected String acquireLockQuery(NextValueRequest request) {
		StringBuilder queryBuilder = new StringBuilder();
		IdSourceKeyMetadata metadata = request.getKey().getMetadata();
		queryBuilder.append( "MERGE (n" );
		queryBuilder.append( labels( metadata.getName(), NodeLabel.TABLE_BASED_SEQUENCE.name() ) );
		queryBuilder.append( " { " );
		queryBuilder.append( metadata.getKeyColumnName() );
		queryBuilder.append( ": {" );
		queryBuilder.append( SEQUENCE_NAME_QUERY_PARAM );
		queryBuilder.append( "}} ) " );
		queryBuilder.append( " ON MATCH SET n.__locked=true " );
		queryBuilder.append( " ON CREATE SET n.__locked=true, n." );
		queryBuilder.append( metadata.getValueColumnName() );
		queryBuilder.append( " = " );
		queryBuilder.append( request.getInitialValue() );
		queryBuilder.append( " RETURN n." );
		queryBuilder.append( metadata.getValueColumnName() );
		String query = queryBuilder.toString();
		return query;
	}

	private void getUpdateTableSequenceQuery(List<Statement> statements, NextValueRequest request) {
		Map<String, Object> params = params( request );

		// Acquire lock
		String acquireLockQuery = acquireLockQuery( request );
		Statement acquireLockStatement = new Statement( acquireLockQuery, params );
		statements.add( acquireLockStatement );

		// Update value
		String updateQuery = increaseQuery( request );
		Statement updateStatement = new Statement( updateQuery, params );
		statements.add( updateStatement );
	}

	protected String increaseQuery(NextValueRequest request) {
		StringBuilder queryBuilder = new StringBuilder();
		IdSourceKeyMetadata metadata = request.getKey().getMetadata();
		Label generatorKeyLabel = DynamicLabel.label( metadata.getName() );
		queryBuilder.append( " MATCH (n " );
		queryBuilder.append( labels( generatorKeyLabel.name(), NodeLabel.TABLE_BASED_SEQUENCE.name() ) );
		queryBuilder.append( " { " );
		queryBuilder.append( metadata.getKeyColumnName() );
		queryBuilder.append( ": {" );
		queryBuilder.append( SEQUENCE_NAME_QUERY_PARAM );
		queryBuilder.append( "}} )" );
		queryBuilder.append( " SET n." );
		queryBuilder.append( metadata.getValueColumnName() );
		queryBuilder.append( " = n." );
		queryBuilder.append( metadata.getValueColumnName() );
		queryBuilder.append( " + " );
		queryBuilder.append( request.getIncrement() );
		queryBuilder.append( " REMOVE n.__locked RETURN n." );
		queryBuilder.append( metadata.getValueColumnName() );
		String query = queryBuilder.toString();
		return query;
	}

	/**
	 * Generate the next value in a sequence for a given {@link IdSourceKey}.
	 *
	 * @return the next value in a sequence
	 */
	@Override
	public Long nextValue(NextValueRequest request) {
		String sequenceName = sequenceName( request.getKey() );
		// This method return 2 statements: the first one to acquire a lock and the second one to update the sequence node
		List<Statement> statements = updateNextValueQuery( request );
		StatementResult result = null;
		Session session = null;
		try {
			session = driver.session();
			Transaction tx = null;
			try {
				tx = session.beginTransaction();
				result = BoltStatementsRunner.runAllReturnLast( tx, statements );
				tx.success();
			}
			finally {
				close( tx );
			}
		}
		finally {
			close( session );
		}

		if ( request.getKey().getMetadata().getType() == IdSourceType.SEQUENCE ) {
			if ( !result.hasNext() ) {
				throw logger.sequenceNotFound( sequenceName );
			}
		}

		// The only way I found to make it work in a multi-threaded environment is to first increment the value and then read it.
		// Our API allows for an initial value and to make sure that I'm actually reading the correct one,
		// the first time I need to decrement the value I obtain from the db.
		Number nextValue = result.single().get( 0 ).asNumber();
		return nextValue.longValue() - request.getIncrement();
	}

	private void close(Resource closable) {
		if ( closable != null ) {
			closable.close();
		}
	}

	/*
	 * This will always return 2 statements: the first one to acquire a lock and the second one to update the sequence value
	 */
	private List<Statement> updateNextValueQuery(NextValueRequest request) {
		return request.getKey().getMetadata().getType() == IdSourceType.TABLE
				? getTableQuery( request )
				: getSequenceIncrementQuery( request );
	}

	private List<Statement> getSequenceIncrementQuery(NextValueRequest request) {
		// Acquire a lock on the node
		String sequenceName = sequenceName( request.getKey() );
		Statement lockStatement = new Statement( SEQUENCE_LOCK_QUERY, Collections.<String, Object>singletonMap( SEQUENCE_NAME_QUERY_PARAM, sequenceName ) );
		List<Statement> statements = new ArrayList<>(2);
		statements.add( lockStatement );

		// Increment the value on the node
		String query = SEQUENCE_VALUE_QUERY.replace( "{increment}", String.valueOf( request.getIncrement() ) ).replace( "{initialValue}", String.valueOf( request.getInitialValue() ) );
		Statement statement = new Statement( query, params( request ) );
		statements.add( statement );
		return statements;
	}

	private List<Statement> getTableQuery(NextValueRequest request) {
		String key = key( request );
		List<Statement> statements = queryCache.get( key );
		if ( statements == null ) {
			statements = new ArrayList<Statement>();
			getUpdateTableSequenceQuery( statements, request );
			List<Statement> cached = queryCache.putIfAbsent( key, statements );
			if ( cached != null ) {
				statements = cached;
			}
		}
		return statements;
	}

	public void createUniqueConstraintsForTableSequences(List<Statement> statements, Iterable<IdSourceKeyMetadata> tableIdGenerators) {
		for ( IdSourceKeyMetadata idSourceKeyMetadata : tableIdGenerators ) {
			if ( idSourceKeyMetadata.getType() == IdSourceType.TABLE ) {
				addUniqueConstraintForTableBasedSequence( statements, idSourceKeyMetadata );
			}
		}
	}
}
