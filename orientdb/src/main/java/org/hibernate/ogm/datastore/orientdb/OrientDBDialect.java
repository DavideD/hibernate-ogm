/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.orientdb;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.hibernate.StaleObjectStateException;
import org.hibernate.ogm.datastore.orientdb.constant.OrientDBConstant;
import org.hibernate.ogm.datastore.orientdb.dialect.impl.OrientDBAssociationQueries;
import org.hibernate.ogm.datastore.orientdb.dialect.impl.OrientDBAssociationSnapshot;
import org.hibernate.ogm.datastore.orientdb.dialect.impl.OrientDBEntityQueries;
import org.hibernate.ogm.datastore.orientdb.dialect.impl.OrientDBTupleAssociationSnapshot;
import org.hibernate.ogm.datastore.orientdb.dialect.impl.OrientDBTupleSnapshot;
import org.hibernate.ogm.datastore.orientdb.dialect.impl.ResultSetTupleIterator;
import org.hibernate.ogm.datastore.orientdb.impl.OrientDBDatastoreProvider;
import org.hibernate.ogm.datastore.orientdb.impl.OrientDBSchemaDefiner;
import org.hibernate.ogm.datastore.orientdb.logging.impl.Log;
import org.hibernate.ogm.datastore.orientdb.logging.impl.LoggerFactory;
import org.hibernate.ogm.datastore.orientdb.query.impl.OrientDBParameterMetadataBuilder;
import org.hibernate.ogm.datastore.orientdb.type.spi.ORecordIdGridType;
import org.hibernate.ogm.datastore.orientdb.type.spi.ORidBagGridType;
import org.hibernate.ogm.datastore.orientdb.utils.EntityKeyUtil;
import org.hibernate.ogm.datastore.orientdb.utils.SequenceUtil;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.ogm.dialect.identity.spi.IdentityColumnAwareGridDialect;
import org.hibernate.ogm.dialect.query.spi.BackendQuery;
import org.hibernate.ogm.dialect.query.spi.ClosableIterator;
import org.hibernate.ogm.dialect.query.spi.ParameterMetadataBuilder;
import org.hibernate.ogm.dialect.query.spi.QueryParameters;
import org.hibernate.ogm.dialect.query.spi.QueryableGridDialect;
import org.hibernate.ogm.dialect.query.spi.TypedGridValue;
import org.hibernate.ogm.dialect.spi.AssociationContext;
import org.hibernate.ogm.dialect.spi.AssociationTypeContext;
import org.hibernate.ogm.dialect.spi.BaseGridDialect;
import org.hibernate.ogm.dialect.spi.ModelConsumer;
import org.hibernate.ogm.dialect.spi.NextValueRequest;
import org.hibernate.ogm.dialect.spi.SessionFactoryLifecycleAwareDialect;
import org.hibernate.ogm.dialect.spi.TupleAlreadyExistsException;
import org.hibernate.ogm.dialect.spi.TupleContext;
import org.hibernate.ogm.model.key.spi.AssociatedEntityKeyMetadata;
import org.hibernate.ogm.model.key.spi.AssociationKey;
import org.hibernate.ogm.model.key.spi.AssociationKeyMetadata;
import org.hibernate.ogm.model.key.spi.EntityKey;
import org.hibernate.ogm.model.key.spi.EntityKeyMetadata;
import org.hibernate.ogm.model.key.spi.IdSourceKeyMetadata.IdSourceType;
import org.hibernate.ogm.model.key.spi.RowKey;
import org.hibernate.ogm.model.spi.Association;
import org.hibernate.ogm.model.spi.AssociationOperation;
import org.hibernate.ogm.model.spi.Tuple;
import org.hibernate.ogm.persister.impl.OgmCollectionPersister;
import org.hibernate.ogm.persister.impl.OgmEntityPersister;
import org.hibernate.ogm.type.spi.GridType;
import org.hibernate.persister.collection.CollectionPersister;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.type.Type;

import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import org.hibernate.ogm.datastore.orientdb.constant.OrientDBMapping;
import org.hibernate.ogm.datastore.orientdb.utils.InsertQueryGenerator;
import org.hibernate.ogm.datastore.orientdb.utils.UpdateQueryGenerator;
import org.hibernate.ogm.datastore.orientdb.dto.GenerationResult;
import org.hibernate.ogm.datastore.orientdb.utils.QueryTypeDefiner;
import org.hibernate.ogm.datastore.orientdb.utils.QueryTypeDefiner.QueryType;
import org.hibernate.ogm.model.key.spi.AssociationKind;

/**
 * Implementation of dialect for OrientDB
 * <p>
 * A {@link Tuple} is saved as a {@link ODocument} where the columns are converted into properties of the node.<br>
 * In the version, an {@link Association} is stored like relation DBMS and identified by the {@link AssociationKey} and
 * the {@link RowKey}. The type of the relationship is the value returned by
 * {@link AssociationKeyMetadata#getCollectionRole()}.
 * <p>
 * If the value of a property is set to null the property will be removed (OrientDB does not allow to store null
 * values).
 *
 * @see QueryableGridDialect
 * @see SessionFactoryLifecycleAwareDialect
 * @see IdentityColumnAwareGridDialect
 * @author Sergey Chernolyas &lt;sergey.chernolyas@gmail.com&gt;
 */
public class OrientDBDialect extends BaseGridDialect implements QueryableGridDialect<String>,
SessionFactoryLifecycleAwareDialect, IdentityColumnAwareGridDialect {

	private static final long serialVersionUID = 1L;
	private static final Log log = LoggerFactory.getLogger();
	private static final Association ASSOCIATION_NOT_FOUND = null;
	private static final InsertQueryGenerator INSERT_QUERY_GENERATOR = new InsertQueryGenerator();
	private static final UpdateQueryGenerator UPDATE_QUERY_GENERATOR = new UpdateQueryGenerator();

	private final OrientDBDatastoreProvider provider;
	private Map<AssociationKeyMetadata, OrientDBAssociationQueries> associationQueries;
	private Map<EntityKeyMetadata, OrientDBEntityQueries> entityQueries;

	/**
	 * Contractor
	 *
	 * @param provider database provider
	 * @see OrientDBDatastoreProvider
	 */

	public OrientDBDialect(OrientDBDatastoreProvider provider) {
		this.provider = provider;
	}

	@Override
	public Tuple getTuple(EntityKey key, TupleContext tupleContext) {
		log.debugf( "getTuple:EntityKey: %s ; tupleContext: %s; current thread: %s ", key, tupleContext, Thread.currentThread().getName() );
		Map<String, Object> dbValuesMap = entityQueries.get( key.getMetadata() ).findEntity( provider.getConnection(), key );
		if ( dbValuesMap == null || dbValuesMap.isEmpty() ) {
			return null;
		}
		return new Tuple(
				new OrientDBTupleSnapshot( dbValuesMap, tupleContext.getAllAssociatedEntityKeyMetadata(), tupleContext.getAllRoles(), key.getMetadata() ) );

	}

	@Override
	public Tuple createTuple(EntityKey key, TupleContext tupleContext) {
		log.debugf( "createTuple:EntityKey: %s ; tupleContext: %s ", key, tupleContext );
		return new Tuple( new OrientDBTupleSnapshot( tupleContext.getAllAssociatedEntityKeyMetadata(), tupleContext.getAllRoles(), key.getMetadata() ) );
	}

	@Override
	public void forEachTuple(ModelConsumer consumer, TupleContext tupleContext, EntityKeyMetadata entityKeyMetadata) {
		throw new UnsupportedOperationException( "Not supported yet." );
	}

	@Override
	public Tuple createTuple(EntityKeyMetadata entityKeyMetadata, TupleContext tupleContext) {
		log.debugf( "createTuple:EntityKeyMetadata: %s ; tupleContext: ", entityKeyMetadata, tupleContext );
		return new Tuple( new OrientDBTupleSnapshot( tupleContext.getAllAssociatedEntityKeyMetadata(), tupleContext.getAllRoles(), entityKeyMetadata ) );
	}

	@Override
	public void insertOrUpdateTuple(EntityKey key, Tuple tuple, TupleContext tupleContext) throws TupleAlreadyExistsException {

		log.debugf( "insertOrUpdateTuple:EntityKey: %s ; tupleContext: %s ; tuple: %s ; thread: %s",
				key, tupleContext, tuple, Thread.currentThread().getName() );
		Connection connection = provider.getConnection();
		OrientDBTupleSnapshot snapshot = (OrientDBTupleSnapshot) tuple.getSnapshot();
		boolean existsInDB = EntityKeyUtil.existsPrimaryKeyInDB( connection, key );
		QueryType queryType = QueryTypeDefiner.define( existsInDB, snapshot.isNew() );
		log.debugf( "insertOrUpdateTuple: snapshot.isNew(): %b ,snapshot.isEmpty(): %b; exists in DB: %b; query type: %s ",
				snapshot.isNew(), snapshot.isEmpty(), existsInDB, queryType );

		StringBuilder queryBuffer = new StringBuilder( 100 );
		switch ( queryType ) {
			case INSERT:
				log.debugf( "insertOrUpdateTuple:Key: %s is new! Insert new record!", key );
				GenerationResult insertResult = INSERT_QUERY_GENERATOR.generate( key.getTable(), tuple, true,
						new HashSet<>( Arrays.asList( key.getColumnNames() ) ) );
				queryBuffer.append( insertResult.getExecutionQuery() );
				break;
			case UPDATE:
				Integer currentVersion = (Integer) snapshot.get( OrientDBConstant.SYSTEM_VERSION );
				log.debugf( "insertOrUpdateTuple:@version: %s. current tread: %s;",
						snapshot.get( OrientDBConstant.SYSTEM_VERSION ), Thread.currentThread().getName() );
				GenerationResult updateResult = UPDATE_QUERY_GENERATOR.generate( key.getTable(), tuple, key, currentVersion );
				queryBuffer.append( updateResult.getExecutionQuery() );
				break;
			case ERROR:
				throw new StaleObjectStateException( key.getTable(), (Serializable) EntityKeyUtil.generatePrimaryKeyPredicate( key ) );
		}

		try {
			log.debugf( "insertOrUpdateTuple:Key: %s; Query: %s; ", key, queryBuffer );
			PreparedStatement pstmt = connection.prepareStatement( queryBuffer.toString() );
			int updateCount = pstmt.executeUpdate();
			log.debugf( "insertOrUpdateTuple:Key: %s ;inserted or updated: %d ", key, updateCount );
			if ( updateCount == 0 && queryType.equals( QueryType.UPDATE ) ) {
				// primary key was in DB .... but during prepare query someone remove it from DB.
				Integer currentVersion = (Integer) snapshot.get( OrientDBConstant.SYSTEM_VERSION );
				throw log.versionNotActual( key, currentVersion );
			}
		}
		catch (SQLException sqle) {
			throw log.cannotExecuteQuery( queryBuffer.toString(), sqle );
		}
		catch (OConcurrentModificationException cme) {
			throw new StaleObjectStateException( key.getTable(), (Serializable) EntityKeyUtil.generatePrimaryKeyPredicate( key ) );
		}
	}

	@Override
	public void insertTuple(EntityKeyMetadata entityKeyMetadata, Tuple tuple, TupleContext tupleContext) {
		log.debugf( "insertTuple:EntityKeyMetadata: %s ; tupleContext: %s ; tuple: %s ",
				entityKeyMetadata, tupleContext, tuple );

		String dbKeyName = entityKeyMetadata.getColumnNames()[0];
		Long dbKeyValue = null;
		Connection connection = provider.getConnection();
		String query = null;

		if ( dbKeyName.equals( OrientDBConstant.SYSTEM_RID ) ) {
			// use @RID for key
			throw new UnsupportedOperationException( "Can not use @RID as primary key!" );
		}
		else {
			// use business key. get new id from sequence
			String seqName = OrientDBSchemaDefiner.generateSeqName( entityKeyMetadata.getTable(), dbKeyName );
			dbKeyValue = (Long) SequenceUtil.getNextSequenceValue( connection, seqName );
			tuple.put( dbKeyName, dbKeyValue );
		}
		GenerationResult result = INSERT_QUERY_GENERATOR.generate( entityKeyMetadata.getTable(), tuple, true,
				new HashSet<>( Arrays.asList( entityKeyMetadata.getColumnNames() ) ) );
		query = result.getExecutionQuery();

		log.debugf( "insertTuple: insertQuery: %s ", result.getExecutionQuery() );
		try {
			PreparedStatement pstmt = connection.prepareStatement( result.getExecutionQuery() );
			log.debugf( "insertTuple:Key: %s (%s) ;inserted or updated: %d ", dbKeyName, dbKeyValue, pstmt.executeUpdate() );
		}
		catch (SQLException sqle) {
			throw log.cannotExecuteQuery( query, sqle );
		}
	}

	@Override
	public void removeTuple(EntityKey key, TupleContext tupleContext) {
		log.debugf( "removeTuple:EntityKey: %s ; tupleContext %s ; current thread: %s",
				key, tupleContext, Thread.currentThread().getName() );
		Connection connection = provider.getConnection();
		StringBuilder queryBuffer = new StringBuilder( 100 );
		try {
			queryBuffer.append( "DELETE VERTEX " ).append( key.getTable() ).append( " where " ).append( EntityKeyUtil.generatePrimaryKeyPredicate( key ) );
			log.debugf( "removeTuple:Key: %s. query: %s ", key, queryBuffer );
			PreparedStatement pstmt = connection.prepareStatement( queryBuffer.toString() );
			log.debugf( "removeTuple:Key: %s. remove: %s", key, pstmt.executeUpdate() );
		}
		catch (SQLException e) {
			throw log.cannotExecuteQuery( queryBuffer.toString(), e );
		}
	}

	@Override
	public Association getAssociation(AssociationKey associationKey, AssociationContext associationContext) {
		log.debugf( "getAssociation:AssociationKey: %s ; AssociationContext: %s", associationKey, associationContext );
		EntityKey entityKey = associationKey.getEntityKey();
		Connection connection = provider.getConnection();
		boolean existsPrimaryKey = EntityKeyUtil.existsPrimaryKeyInDB( connection, entityKey );
		if ( !existsPrimaryKey ) {
			// Entity now extists
			return ASSOCIATION_NOT_FOUND;
		}
		Map<RowKey, Tuple> tuples = createAssociationMap( associationKey, associationContext );
		log.debugf( "getAssociation:tuples keys: %s ; ", tuples.keySet() );
		return new Association( new OrientDBAssociationSnapshot( tuples ) );
	}

	private Map<RowKey, Tuple> createAssociationMap(AssociationKey associationKey, AssociationContext associationContext) {
		log.debugf( "createAssociationMap:AssociationKey: %s ; AssociationContext: %s", associationKey, associationContext );
		Connection connection = provider.getConnection();
		List<Map<String, Object>> relationships = entityQueries.get( associationKey.getEntityKey().getMetadata() )
				.findAssociation( connection, associationKey, associationContext );

		Map<RowKey, Tuple> tuples = new LinkedHashMap<>();

		for ( Map<String, Object> relationship : relationships ) {
			OrientDBTupleAssociationSnapshot snapshot = new OrientDBTupleAssociationSnapshot( relationship, associationKey,
					associationContext );
			tuples.put( convertToRowKey( associationKey, snapshot ), new Tuple( snapshot ) );
		}
		return tuples;
	}

	private RowKey convertToRowKey(AssociationKey associationKey, OrientDBTupleAssociationSnapshot snapshot) {
		String[] columnNames = associationKey.getMetadata().getRowKeyColumnNames();
		Object[] values = new Object[columnNames.length];
		for ( int i = 0; i < columnNames.length; i++ ) {
			String columnName = columnNames[i];
			values[i] = snapshot.get( columnName );
		}
		return new RowKey( columnNames, values );
	}

	@Override
	public Association createAssociation(AssociationKey associationKey, AssociationContext associationContext) {
		log.debugf( "createAssociation: %s ; AssociationContext: %s", associationKey, associationContext );
		return new Association();
	}

	@Override
	public void insertOrUpdateAssociation(AssociationKey associationKey, Association association, AssociationContext associationContext) {
		log.debugf( "insertOrUpdateAssociation: AssociationKey: %s ; AssociationContext: %s ; association: %s", associationKey, associationContext,
				association );
		log.debugf( "insertOrUpdateAssociation: EntityKey: %s ;", associationKey.getEntityKey() );
		log.debugf( "insertOrUpdateAssociation: operations: %s ;", association.getOperations() );

		for ( AssociationOperation action : association.getOperations() ) {
			applyAssociationOperation( association, associationKey, action, associationContext );
		}

	}

	private void applyAssociationOperation(Association association, AssociationKey associationKey, AssociationOperation operation,
			AssociationContext associationContext) {
		switch ( operation.getType() ) {
			case CLEAR:
				log.debugf( "applyAssociationOperation: CLEAR operation for: %s ;", associationKey );
				removeAssociation( associationKey, associationContext );
				break;
			case PUT:
				log.debugf( "applyAssociationOperation: PUT operation for: %s ;", associationKey );
				putAssociationOperation( association, associationKey, operation,
						associationContext.getAssociationTypeContext().getAssociatedEntityKeyMetadata() );
				break;
			case REMOVE:
				log.debugf( "applyAssociationOperation: REMOVE operation for: %s ;", associationKey );
				removeAssociationOperation( association, associationKey, operation,
						associationContext.getAssociationTypeContext().getAssociatedEntityKeyMetadata() );
				break;
		}
	}

	@Override
	public void removeAssociation(AssociationKey key, AssociationContext associationContext) {
		// Remove the list of tuples corresponding to a given association
		// If this is the inverse side of a bi-directional association, we don't manage the relationship from this side
		if ( key.getMetadata().isInverse() ) {
			return;
		}

		associationQueries.get( key.getMetadata() ).removeAssociation( provider.getConnection(), key, associationContext );
	}

	private void removeAssociationOperation(Association association, AssociationKey associationKey, AssociationOperation action,
			AssociatedEntityKeyMetadata associatedEntityKeyMetadata) {
		log.debugf( "removeAssociationOperation: action key: %s ;action value: %s ; metadata: %s; association:%s;associatedEntityKeyMetadata:%s",
				action.getKey(), action.getValue(), associationKey.getMetadata(), association, associatedEntityKeyMetadata );
		log.debugf( "removeAssociationOperation: contains key :%b",
				associationQueries.containsKey( associationKey.getMetadata() ) );
		if ( associationKey.getMetadata().isInverse() ) {
			return;
		}
		associationQueries.get( associationKey.getMetadata() ).removeAssociationRow( provider.getConnection(), associationKey, action.getKey() );
	}

	private void putAssociationOperation(Association association, AssociationKey associationKey, AssociationOperation action,
			AssociatedEntityKeyMetadata associatedEntityKeyMetadata) {
		log.debugf( "putAssociationOperation: : action: %s ; metadata: %s; association:%s", action, associationKey.getMetadata(), association );
		Connection connection = provider.getConnection();
		if ( associationQueries.containsKey( associationKey.getMetadata() ) ) {
			List<Map<String, Object>> relationship = associationQueries.get( associationKey.getMetadata() ).findRelationship( connection,
					associationKey, action.getKey() );
			if ( relationship.isEmpty() ) {
				// create
				createRelationshipWithNode( associationKey, action.getValue(), associatedEntityKeyMetadata );
			}
			else {
				log.debugf( "putAssociationOperation: :  associationKey: %s ", associationKey );
				log.debugf( "putAssociationOperation: :  associations for  metadata: %s is %d", associationKey.getMetadata(), relationship.size() );
				updateRelationshipWithNode( associationKey, action.getValue(), associatedEntityKeyMetadata );
			}
		}
		else {
			log.debugf( "putAssociationOperation: no associations for  metadata: %s", associationKey.getMetadata() );
		}

	}

	private void createRelationshipWithNode(AssociationKey associationKey, Tuple associationRow,
			AssociatedEntityKeyMetadata associatedEntityKeyMetadata) {
		log.debugf( "createRelationshipWithEntityNode: associationKey.getMetadata(): %s ; associationRow: %s ; associatedEntityKeyMetadata: %s",
				associationKey.getMetadata(), associationRow, associatedEntityKeyMetadata );
		GenerationResult result = INSERT_QUERY_GENERATOR.generate( associationKey.getTable(), associationRow, false, Collections.<String>emptySet() );
		log.debugf( "createRelationshipWithNode: query: %s", result.getExecutionQuery() );
		try {
			PreparedStatement pstmt = provider.getConnection().prepareStatement( result.getExecutionQuery() );
			log.debugf( "createRelationshipWithNode: execute insert query: %d", pstmt.executeUpdate() );
		}
		catch (SQLException sqle) {
			throw log.cannotExecuteQuery( result.getExecutionQuery(), sqle );
		}
	}

	private void updateRelationshipWithNode(AssociationKey associationKey, Tuple associationRow,
			AssociatedEntityKeyMetadata associatedEntityKeyMetadata) {
		log.debugf( "updateRelationshipWithNode: associationKey.getMetadata(): %s ; associationRow: %s ; associatedEntityKeyMetadata: %s",
				associationKey.getMetadata(), associationRow, associatedEntityKeyMetadata );
		if ( AssociationKind.EMBEDDED_COLLECTION.equals( associationKey.getMetadata().getAssociationKind() ) ) {
			GenerationResult result = UPDATE_QUERY_GENERATOR.generate( associationKey, associationRow );
			log.debugf( "updateRelationshipWithNode: query: %s", result.getExecutionQuery() );
			try {
				PreparedStatement pstmt = provider.getConnection().prepareStatement( result.getExecutionQuery() );
				log.debugf( "updateRelationshipWithNode: execute update query: %d", pstmt.executeUpdate() );
			}
			catch (SQLException sqle) {
				throw log.cannotExecuteQuery( result.getExecutionQuery(), sqle );
			}
		}
		else {
			log.debugf( "updateRelationshipWithNode: update  association  not needs!" );
		}
	}

	@Override
	public boolean isStoredInEntityStructure(AssociationKeyMetadata associationKeyMetadata, AssociationTypeContext associationTypeContext) {
		return true;
	}

	@Override
	public Number nextValue(NextValueRequest request) {
		log.debugf( "NextValueRequest: %s", request );
		long nextValue = 0;
		Connection connection = provider.getConnection();
		IdSourceType type = request.getKey().getMetadata().getType();
		switch ( type ) {
			case SEQUENCE:
				String seqName = request.getKey().getMetadata().getName();
				nextValue = SequenceUtil.getNextSequenceValue( connection, seqName );
				break;
			case TABLE:
				String seqTableName = "sequences";
				String pkColumnName = "key";
				String valueColumnName = "seed";
				String pkColumnValue = (String) request.getKey().getColumnValues()[0];
				log.debugf( "seqTableName:%s, pkColumnName:%s, pkColumnValue:%s, valueColumnName:%s",
						seqTableName, pkColumnName, pkColumnValue, valueColumnName );
				nextValue = SequenceUtil.getNextTableValue( connection, seqTableName, pkColumnName, pkColumnValue, valueColumnName,
						request.getInitialValue(), request.getIncrement() );
		}
		log.debugf( "nextValue: %d", nextValue );
		return nextValue;
	}

	@Override
	public boolean supportsSequences() {
		return true;
	}

	/**
	 * Prepare PreparedStemenet for execute NoSQL query
	 *
	 * @param backendQuery represention of NoSQL query
	 * @param queryParameters parameters of the query
	 * @return Prepared PreparedStatement
	 * @throws SQLException if any database exception occurs
	 */

	@SuppressWarnings("unchecked")
	private PreparedStatement prepareStatement(BackendQuery<String> backendQuery, QueryParameters queryParameters) throws SQLException {
		Map<String, Object> parameters = getNamedParameterValuesConvertedByGridType( queryParameters );
		log.debugf( "prepareStatement: parameters: %s ; ",
				parameters.keySet() );
		String nativeQuery = backendQuery.getQuery();
		log.debugf( "prepareStatement: nativeQuery: %s ; metadata: %s",
				backendQuery.getQuery(), backendQuery.getSingleEntityMetadataInformationOrNull() );
		PreparedStatement pstmt = provider.getConnection().prepareStatement( nativeQuery );
		int paramIndex = 1;
		for ( Map.Entry<String, TypedGridValue> entry : queryParameters.getNamedParameters().entrySet() ) {
			String key = entry.getKey();
			TypedGridValue value = entry.getValue();
			log.debugf( "prepareStatement: key: %s ; type: %s ; value: %s; type class: %s ",
					key, value.getType(), value.getValue(), value.getType().getReturnedClass() );
			try {
				if ( OrientDBMapping.SIMPLE_VALUE_SETTER_MAP.containsKey( value.getType() ) ) {
					OrientDBMapping.SIMPLE_VALUE_SETTER_MAP.get( value.getType() ).setValue( pstmt, paramIndex, value.getValue() );
				}
				else {
					throw new UnsupportedOperationException( "Type " + value.getType() + " is not supported!" );
				}

			}
			catch (SQLException sqle) {
				throw log.cannotSetValueForParameter( paramIndex, sqle );
			}
			paramIndex++;
		}
		return pstmt;
	}

	@SuppressWarnings("unchecked")
	@Override
	public int executeBackendUpdateQuery(BackendQuery<String> backendQuery, QueryParameters queryParameters) {
		try {
			PreparedStatement pstmt = prepareStatement( backendQuery, queryParameters );
			return pstmt.executeUpdate();
		}
		catch (SQLException e) {
			throw log.cannotExecuteQuery( backendQuery.getQuery(), e );
		}
	}

	@Override
	public ClosableIterator<Tuple> executeBackendQuery(BackendQuery<String> backendQuery, QueryParameters queryParameters) {
		try {
			PreparedStatement pstmt = prepareStatement( backendQuery, queryParameters );
			return new ResultSetTupleIterator( pstmt.executeQuery() );
		}
		catch (SQLException e) {
			throw log.cannotExecuteQuery( backendQuery.getQuery(), e );
		}
	}

	/**
	 * Returns a map with the named parameter values from the given parameters object, converted by the {@link GridType}
	 * corresponding to each parameter type.
	 */
	private Map<String, Object> getNamedParameterValuesConvertedByGridType(QueryParameters queryParameters) {
		Map<String, Object> parameterValues = new LinkedHashMap<>( queryParameters.getNamedParameters().size() );
		Tuple dummy = new Tuple();
		for ( Map.Entry<String, TypedGridValue> parameter : queryParameters.getNamedParameters().entrySet() ) {
			parameter.getValue().getType().nullSafeSet( dummy, parameter.getValue().getValue(), new String[]{ parameter.getKey() }, null );
			parameterValues.put( parameter.getKey(), dummy.get( parameter.getKey() ) );
		}
		return parameterValues;
	}

	@Override
	public ParameterMetadataBuilder getParameterMetadataBuilder() {
		return new OrientDBParameterMetadataBuilder();
	}

	@Override
	public String parseNativeQuery(String nativeQuery) {
		return nativeQuery;

	}

	@Override
	public void sessionFactoryCreated(SessionFactoryImplementor sessionFactoryImplementor) {
		this.associationQueries = initializeAssociationQueries( sessionFactoryImplementor );
		this.entityQueries = initializeEntityQueries( sessionFactoryImplementor, associationQueries );
	}

	/**
	 * add queries for associate entities
	 *
	 * @param sessionFactoryImplementor session factory
	 * @param associationQueries map with association
	 * @return map between {@link EntityKeyMetadata} and {@link OrientDBEntityQueries}
	 * @see EntityKeyMetadata
	 * @see OrientDBEntityQueries
	 */
	private Map<EntityKeyMetadata, OrientDBEntityQueries> initializeEntityQueries(SessionFactoryImplementor sessionFactoryImplementor,
			Map<AssociationKeyMetadata, OrientDBAssociationQueries> associationQueries) {
		Map<EntityKeyMetadata, OrientDBEntityQueries> entityQueries = initializeEntityQueries( sessionFactoryImplementor );
		for ( AssociationKeyMetadata associationKeyMetadata : associationQueries.keySet() ) {
			EntityKeyMetadata entityKeyMetadata = associationKeyMetadata.getAssociatedEntityKeyMetadata().getEntityKeyMetadata();
			if ( !entityQueries.containsKey( entityKeyMetadata ) ) {
				// Embeddables metadata
				entityQueries.put( entityKeyMetadata, new OrientDBEntityQueries( entityKeyMetadata ) );
			}
		}
		return entityQueries;
	}

	/**
	 * initialize queries for CRUD entities
	 *
	 * @param sessionFactoryImplementor session factory
	 * @return map between {@link EntityKeyMetadata} and {@link OrientDBEntityQueries}
	 * @see EntityKeyMetadata
	 * @see OrientDBEntityQueries
	 */
	private Map<EntityKeyMetadata, OrientDBEntityQueries> initializeEntityQueries(SessionFactoryImplementor sessionFactoryImplementor) {
		Map<EntityKeyMetadata, OrientDBEntityQueries> queryMap = new HashMap<>();
		Collection<EntityPersister> entityPersisters = sessionFactoryImplementor.getEntityPersisters().values();
		for ( EntityPersister entityPersister : entityPersisters ) {
			if ( entityPersister instanceof OgmEntityPersister ) {
				OgmEntityPersister ogmEntityPersister = (OgmEntityPersister) entityPersister;
				queryMap.put( ogmEntityPersister.getEntityKeyMetadata(), new OrientDBEntityQueries( ogmEntityPersister.getEntityKeyMetadata() ) );
			}
		}
		return queryMap;
	}

	/**
	 * initialize queries for associate entities
	 *
	 * @param sessionFactoryImplementor session factory
	 * @return map between {@link AssociationKeyMetadata} and {@link OrientDBAssociationQueries}
	 * @see AssociationKeyMetadata
	 * @see OrientDBAssociationQueries
	 */
	private Map<AssociationKeyMetadata, OrientDBAssociationQueries> initializeAssociationQueries(SessionFactoryImplementor sessionFactoryImplementor) {
		Map<AssociationKeyMetadata, OrientDBAssociationQueries> queryMap = new HashMap<>();
		Collection<CollectionPersister> collectionPersisters = sessionFactoryImplementor.getCollectionPersisters().values();
		for ( CollectionPersister collectionPersister : collectionPersisters ) {
			if ( collectionPersister instanceof OgmCollectionPersister ) {
				OgmCollectionPersister ogmCollectionPersister = (OgmCollectionPersister) collectionPersister;

				log.debugf( "initializeAssociationQueries: ogmCollectionPersister : %s", ogmCollectionPersister );
				EntityKeyMetadata ownerEntityKeyMetadata = ( (OgmEntityPersister) ( ogmCollectionPersister.getOwnerEntityPersister() ) ).getEntityKeyMetadata();

				log.debugf( "initializeAssociationQueries: ownerEntityKeyMetadata : %s", ownerEntityKeyMetadata );
				AssociationKeyMetadata associationKeyMetadata = ogmCollectionPersister.getAssociationKeyMetadata();

				log.debugf( "initializeAssociationQueries: associationKeyMetadata : %s", associationKeyMetadata );
				queryMap.put( associationKeyMetadata, new OrientDBAssociationQueries( ownerEntityKeyMetadata, associationKeyMetadata ) );
			}
		}
		return queryMap;
	}

	@Override
	public GridType overrideType(Type type) {
		GridType gridType = null;

		if ( type.getReturnedClass().equals( ORecordId.class ) ) {
			gridType = ORecordIdGridType.INSTANCE;
		}
		else if ( type.getReturnedClass().equals( ORidBag.class ) ) {
			gridType = ORidBagGridType.INSTANCE;
		}
		else {
			gridType = super.overrideType( type );
		}
		return gridType;
	}

}
