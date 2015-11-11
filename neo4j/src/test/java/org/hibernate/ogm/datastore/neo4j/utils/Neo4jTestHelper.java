/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.neo4j.utils;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.ws.rs.core.Response;

import org.fest.util.Files;
import org.hibernate.SessionFactory;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.ogm.datastore.document.options.AssociationStorageType;
import org.hibernate.ogm.datastore.neo4j.Neo4j;
import org.hibernate.ogm.datastore.neo4j.Neo4jDialect;
import org.hibernate.ogm.datastore.neo4j.Neo4jProperties;
import org.hibernate.ogm.datastore.neo4j.RemoteNeo4jDialect;
import org.hibernate.ogm.datastore.neo4j.dialect.impl.NodeLabel;
import org.hibernate.ogm.datastore.neo4j.impl.Neo4jDatastoreProvider;
import org.hibernate.ogm.datastore.neo4j.remote.impl.Neo4jClient;
import org.hibernate.ogm.datastore.neo4j.remote.impl.RemoteNeo4jDatastoreProvider;
import org.hibernate.ogm.datastore.neo4j.remote.json.impl.ErrorResponse;
import org.hibernate.ogm.datastore.neo4j.remote.json.impl.Statement;
import org.hibernate.ogm.datastore.neo4j.remote.json.impl.Statements;
import org.hibernate.ogm.datastore.spi.BaseDatastoreProvider;
import org.hibernate.ogm.datastore.spi.DatastoreConfiguration;
import org.hibernate.ogm.datastore.spi.DatastoreProvider;
import org.hibernate.ogm.dialect.spi.GridDialect;
import org.hibernate.ogm.model.key.spi.EntityKey;
import org.hibernate.ogm.model.spi.TupleSnapshot;
import org.hibernate.ogm.utils.GridDialectOperationContexts;
import org.hibernate.ogm.utils.TestableGridDialect;
import org.hibernate.service.spi.Stoppable;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.ResourceIterator;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * @author Davide D'Alto &lt;davide@hibernate.org&gt;
 */
public class Neo4jTestHelper implements TestableGridDialect {

	private static final  Map<String, String> hibernateProperties = readProperties();

	private static final String ROOT_FOLDER = hibernateProperties.get( Neo4jProperties.DATABASE_PATH ) + File.separator + "NEO4J";

	/**
	 * Query for counting all entities. This takes embedded entities and temporary nodes (which never should show up
	 * actually) into account.
	 */
	private static final String ENTITY_COUNT_QUERY = "MATCH (n) WHERE n:" + NodeLabel.ENTITY.name() + " OR n:" + NodeLabel.EMBEDDED.name() + " RETURN COUNT(n) as count";

	private static final String ASSOCIATION_COUNT_QUERY = "MATCH (n) -[r]-> () RETURN COUNT(DISTINCT type(r)) as count";

	private static final String DELETE_ALL = "MATCH (n) OPTIONAL MATCH (n) -[r]-> () DELETE n,r";

	@Override
	public long getNumberOfEntities(SessionFactory sessionFactory) {
		DatastoreProvider provider = getProvider( sessionFactory );
		if ( isRemote( provider ) ) {
			Neo4jClient remoteNeo4j = ( (RemoteNeo4jDatastoreProvider) provider ).getDataStore();
			Statement statement = new Statement( ENTITY_COUNT_QUERY );
			statement.setResultDataContents( Arrays.asList( Statement.AS_ROW ) );
			return readCountFromResponse( remoteNeo4j, statement );
		}
		else {
			GraphDatabaseService graphDb = ( (Neo4jDatastoreProvider) provider ).getDataBase();
			ResourceIterator<Long> result = graphDb.execute( ENTITY_COUNT_QUERY ).columnAs( "count" );
			Long count = result.next();
			result.close();
			return count.longValue();
		}
	}

	@Override
	public long getNumberOfAssociations(SessionFactory sessionFactory) {
		BaseDatastoreProvider provider = getProvider( sessionFactory );
		if ( isRemote( provider ) ) {
			Neo4jClient remoteNeo4j = ( (RemoteNeo4jDatastoreProvider) getProvider( sessionFactory ) ).getDataStore();
			Statement statement = new Statement( ASSOCIATION_COUNT_QUERY );
			statement.setResultDataContents( Arrays.asList( Statement.AS_ROW ) );
			return readCountFromResponse( remoteNeo4j, statement );
		}
		else {
			GraphDatabaseService graphDb = ( (Neo4jDatastoreProvider) getProvider( sessionFactory ) ).getDataBase();
			ResourceIterator<Long> result = graphDb.execute( ASSOCIATION_COUNT_QUERY ).columnAs( "count" );
			Long count = result.next();
			result.close();
			return count.longValue();
		}
	}

	private long readCountFromResponse(Neo4jClient remoteNeo4j, Statement statement) {
		Response response = remoteNeo4j.executeQuery( statement );
		try {
			RowStatementsResponse readEntity = response.readEntity( RowStatementsResponse.class );
			return ( (Integer) readEntity.getResults().get( 0 ).getData().get( 0 ).getRow().get( 0 ) ).longValue();
		}
		finally {
			response.close();
		}
	}

	@Override
	public Map<String, Object> extractEntityTuple(SessionFactory sessionFactory, EntityKey key) {
		Map<String, Object> tuple = new HashMap<String, Object>();
		GridDialect dialect = getDialect( sessionFactory );
		TupleSnapshot snapshot = dialect.getTuple( key, GridDialectOperationContexts.emptyTupleContext() ).getSnapshot();
		for ( String column : snapshot.getColumnNames() ) {
			tuple.put( column, snapshot.get( column ) );
		}
		return tuple;
	}

	@Override
	public boolean backendSupportsTransactions() {
		return true;
	}

	@Override
	public void dropSchemaAndDatabase(SessionFactory sessionFactory) {
		DatastoreProvider provider = getProvider( sessionFactory );
		if ( provider instanceof RemoteNeo4jDatastoreProvider ) {
			RemoteNeo4jDatastoreProvider remoteProvider = (RemoteNeo4jDatastoreProvider) provider;
			Statements statements = new Statements();
			statements.addStatement( DELETE_ALL );
			remoteProvider.getDataStore().executeQueriesInNewTransaction( statements );
		}
		else {
			( (Stoppable) getProvider( sessionFactory ) ).stop();
			Files.delete( new File( ROOT_FOLDER ) );
		}
	}

	public static void deleteAllElements( DatastoreProvider provider) {
		if ( isRemote( provider ) ) {
			Neo4jClient remoteNeo4j = ( (RemoteNeo4jDatastoreProvider) provider ).getDataStore();
			Statement statement = new Statement( DELETE_ALL );
			statement.setResultDataContents( Arrays.asList( Statement.AS_ROW ) );
			remoteNeo4j.executeQuery( statement ).close();
		}
		else {
			GraphDatabaseService graphDb = ( (Neo4jDatastoreProvider) provider ).getDataBase();
			graphDb.execute( DELETE_ALL ).close();
		}
	}

	@Override
	public Map<String, String> getEnvironmentProperties() {
		Map<String, String> properties = new HashMap<>( hibernateProperties );
		properties.put( Neo4jProperties.DATABASE_PATH, dbLocation() );
		return properties;
	}

	/**
	 * Returns a random location where to create a neo4j database
	 */
	public static String dbLocation() {
		return ROOT_FOLDER + File.separator + "neo4j-db-" + System.currentTimeMillis();
	}

	private static Map<String, String> readProperties() {
		try {
			Properties hibProperties = new Properties();
			hibProperties.load( Thread.currentThread().getContextClassLoader().getResourceAsStream( "hibernate.properties" ) );
			Map<String, String> props = new HashMap<>();
			for ( Map.Entry<Object, Object> entry : hibProperties.entrySet() ) {
				props.put( String.valueOf( entry.getKey() ), String.valueOf( entry.getValue() ) );
			}
			return Collections.unmodifiableMap( props );
		}
		catch (IOException e) {
			throw new RuntimeException( "Missing properties file: hibernate.properties" );
		}
	}

	private static BaseDatastoreProvider getProvider(SessionFactory sessionFactory) {
		DatastoreProvider provider = ( (SessionFactoryImplementor) sessionFactory ).getServiceRegistry().getService( DatastoreProvider.class );
		if ( Neo4jDatastoreProvider.class.isInstance( provider ) ) {
			return Neo4jDatastoreProvider.class.cast( provider );
		}
		if ( RemoteNeo4jDatastoreProvider.class.isInstance( provider ) ) {
			return RemoteNeo4jDatastoreProvider.class.cast( provider );
		}
		throw new RuntimeException( "Not testing with Neo4jDB, cannot extract underlying provider" );
	}

	private static GridDialect getDialect(SessionFactory sessionFactory) {
		GridDialect dialect = ( (SessionFactoryImplementor) sessionFactory ).getServiceRegistry().getService( GridDialect.class );
		return dialect;
	}

	@Override
	public long getNumberOfAssociations(SessionFactory sessionFactory, AssociationStorageType type) {
		throw new UnsupportedOperationException( "This datastore does not support different association storage strategies." );
	}

	@Override
	public Class<? extends DatastoreConfiguration<?>> getDatastoreConfigurationType() {
		return Neo4j.class;
	}

	@Override
	public GridDialect getGridDialect(DatastoreProvider datastoreProvider) {
		if ( Neo4jDatastoreProvider.class.isInstance( datastoreProvider ) ) {
			return new Neo4jDialect( (Neo4jDatastoreProvider) datastoreProvider );
		}
		if ( RemoteNeo4jDatastoreProvider.class.isInstance( datastoreProvider ) ) {
			return new RemoteNeo4jDialect( (RemoteNeo4jDatastoreProvider) datastoreProvider );
		}
		throw new RuntimeException( "Not testing with Neo4jDB, cannot extract underlying dialect" );
	}

	private static boolean isRemote(DatastoreProvider provider) {
		return provider instanceof RemoteNeo4jDatastoreProvider;
	}

	@JsonIgnoreProperties(ignoreUnknown = true)
	public static class RowStatementsResponse {

		private List<RowStatementsResult> results;

		private List<ErrorResponse> errors;

		public List<RowStatementsResult> getResults() {
			return results;
		}

		public void setResults(List<RowStatementsResult> results) {
			this.results = results;
		}

		public List<ErrorResponse> getErrors() {
			return errors;
		}

		public void setErrors(List<ErrorResponse> errors) {
			this.errors = errors;
		}
	}

	@JsonIgnoreProperties(ignoreUnknown = true)
	public static class RowStatementsResult {

		private List<String> columns;

		private List<RowArray> data;

		public List<String> getColumns() {
			return columns;
		}

		public void setColumns(List<String> columns) {
			this.columns = columns;
		}

		public List<RowArray> getData() {
			return data;
		}

		public void setData(List<RowArray> data) {
			this.data = data;
		}
	}

	@JsonIgnoreProperties(ignoreUnknown = true)
	public static class RowArray {

		private List<Object> row;

		public List<Object> getRow() {
			return row;
		}

		public void setRow(List<Object> row) {
			this.row = row;
		}
	}
}
