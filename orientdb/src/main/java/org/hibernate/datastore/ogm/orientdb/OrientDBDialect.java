/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.datastore.ogm.orientdb;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.hibernate.AssertionFailure;
import org.hibernate.StaleObjectStateException;
import org.hibernate.datastore.ogm.orientdb.constant.OrientDBConstant;
import org.hibernate.datastore.ogm.orientdb.dialect.impl.OrientDBAssociationQueries;
import org.hibernate.datastore.ogm.orientdb.dialect.impl.OrientDBAssociationSnapshot;
import org.hibernate.datastore.ogm.orientdb.dialect.impl.OrientDBEntityQueries;
import org.hibernate.datastore.ogm.orientdb.dialect.impl.OrientDBTupleAssociationSnapshot;
import org.hibernate.datastore.ogm.orientdb.dialect.impl.OrientDBTupleSnapshot;
import org.hibernate.datastore.ogm.orientdb.dialect.impl.ResultSetTupleIterator;
import org.hibernate.datastore.ogm.orientdb.impl.OrientDBDatastoreProvider;
import org.hibernate.datastore.ogm.orientdb.impl.OrientDBSchemaDefiner;
import org.hibernate.datastore.ogm.orientdb.logging.impl.Log;
import org.hibernate.datastore.ogm.orientdb.logging.impl.LoggerFactory;
import org.hibernate.datastore.ogm.orientdb.query.impl.OrientDBParameterMetadataBuilder;
import org.hibernate.datastore.ogm.orientdb.type.spi.ORecordIdGridType;
import org.hibernate.datastore.ogm.orientdb.type.spi.ORidBagGridType;
import org.hibernate.datastore.ogm.orientdb.utils.EntityKeyUtil;
import org.hibernate.datastore.ogm.orientdb.utils.SequenceUtil;
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
import org.hibernate.ogm.type.impl.Iso8601StringCalendarType;
import org.hibernate.ogm.type.impl.Iso8601StringDateType;
import org.hibernate.ogm.type.impl.SerializableAsStringType;
import org.hibernate.ogm.type.spi.GridType;
import org.hibernate.persister.collection.CollectionPersister;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.service.spi.ServiceRegistryAwareService;
import org.hibernate.service.spi.ServiceRegistryImplementor;
import org.hibernate.type.SerializableToBlobType;
import org.hibernate.type.StandardBasicTypes;
import org.hibernate.type.Type;

import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.id.ORecordId;
import java.util.Arrays;
import java.util.HashSet;
import org.hibernate.datastore.ogm.orientdb.utils.InsertQueryGenerator;
import org.hibernate.datastore.ogm.orientdb.utils.QueryUtil;
import org.hibernate.datastore.ogm.orientdb.utils.UpdateQueryGenerator;
import org.hibernate.datastore.ogm.orientdb.utils.AbstractQueryGenerator.GenerationResult;
import org.hibernate.datastore.ogm.orientdb.utils.QueryTypeDefiner;
import org.hibernate.datastore.ogm.orientdb.utils.QueryTypeDefiner.QueryType;

/**
 * @author Sergey Chernolyas (sergey.chernolyas@gmail.com)
 */
public class OrientDBDialect extends BaseGridDialect implements QueryableGridDialect<String>,
		ServiceRegistryAwareService, SessionFactoryLifecycleAwareDialect, IdentityColumnAwareGridDialect {

	private static final long serialVersionUID = 1L;
	private static final Log log = LoggerFactory.getLogger();
	private static final Association ASSOCIATION_NOT_FOUND = null;
	private static final InsertQueryGenerator INSERT_QUERY_GENERATOR = new InsertQueryGenerator();
	private static final UpdateQueryGenerator UPDATE_QUERY_GENERATOR = new UpdateQueryGenerator();

	private OrientDBDatastoreProvider provider;
	private ServiceRegistryImplementor serviceRegistry;
	private Map<AssociationKeyMetadata, OrientDBAssociationQueries> associationQueries;
	private Map<EntityKeyMetadata, OrientDBEntityQueries> entityQueries;
        private static final Map<String, ValueSetter> VALUE_SETTER_MAP;
        
        static {
            Map<String, ValueSetter> map = new HashMap<>();
            map.put("sring", new StringValueSetter());
             map.put("long", new LongValueSetter());
              map.put("integer", new IntegerValueSetter());
            VALUE_SETTER_MAP = map;
        }
        

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
	public void forEachTuple(ModelConsumer consumer, EntityKeyMetadata... entityKeyMetadatas) {
		throw new UnsupportedOperationException( "Not supported yet!" );
	}

	@Override
	public Tuple createTuple(EntityKey key, TupleContext tupleContext) {
		log.debugf( "createTuple:EntityKey: %s ; tupleContext: %s ", key, tupleContext );
		return new Tuple( new OrientDBTupleSnapshot( tupleContext.getAllAssociatedEntityKeyMetadata(), tupleContext.getAllRoles(), key.getMetadata() ) );
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

		StringBuilder queryBuffer = new StringBuilder(100);		
		List<Object> preparedStatementParams = Collections.emptyList();

		switch ( queryType ) {
			case INSERT:
				log.debugf( "insertOrUpdateTuple:Key: %s is new! Insert new record!", key );
				GenerationResult insertResult = INSERT_QUERY_GENERATOR.generate( key.getTable(), tuple, true,
						new HashSet<String>( Arrays.asList( key.getColumnNames() ) ) );
				queryBuffer.append( insertResult.getExecutionQuery() );
				preparedStatementParams = insertResult.getPreparedStatementParams();
				break;
			case UPDATE:
				Integer currentVersion = (Integer) snapshot.get( OrientDBConstant.SYSTEM_VERSION );
				log.debugf( "insertOrUpdateTuple:@version: %s. current tread: %s;",
						snapshot.get( OrientDBConstant.SYSTEM_VERSION ), Thread.currentThread().getName() );
				GenerationResult updateResult = UPDATE_QUERY_GENERATOR.generate( key.getTable(), tuple, key, currentVersion );
				queryBuffer.append( updateResult.getExecutionQuery() );
				break;
			case ERROR:
				throw new StaleObjectStateException( key.getTable(), (Serializable) EntityKeyUtil.generatePrimaryKeyPredicate(key) );
		}

		try {
			log.debugf( "insertOrUpdateTuple:Key: %s; Query: %s; ", key, queryBuffer );
			PreparedStatement pstmt = connection.prepareStatement( queryBuffer.toString() );
			log.debugf( "insertOrUpdateTuple: exist parameters for preparedstatement : %d", preparedStatementParams.size() );
			QueryUtil.setParameters( pstmt, preparedStatementParams );
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
			throw new StaleObjectStateException( key.getTable(), (Serializable) EntityKeyUtil.generatePrimaryKeyPredicate(key) );
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
		InsertQueryGenerator.GenerationResult result = INSERT_QUERY_GENERATOR.generate( entityKeyMetadata.getTable(), tuple, true,
				new HashSet<String>( Arrays.asList( entityKeyMetadata.getColumnNames() ) ) );
		query = result.getExecutionQuery();

		log.debugf( "insertTuple: insertQuery: %s ", result.getExecutionQuery() );
		try {
			PreparedStatement pstmt = connection.prepareStatement( result.getExecutionQuery() );
			if ( result.getPreparedStatementParams() != null ) {
				QueryUtil.setParameters( pstmt, result.getPreparedStatementParams() );
			}
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
		StringBuilder queryBuffer = new StringBuilder(100);
		try {
			queryBuffer.append( "DELETE VERTEX " ).append( key.getTable() ).append( " where " ).append( EntityKeyUtil.generatePrimaryKeyPredicate(key) );
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
		return new Association( new OrientDBAssociationSnapshot( tuples ) );
	}

	private Map<RowKey, Tuple> createAssociationMap(AssociationKey associationKey, AssociationContext associationContext) {
		Connection connection = provider.getConnection();
		List<Map<String, Object>> relationships = entityQueries.get( associationKey.getEntityKey().getMetadata() )
				.findAssociation( connection, associationKey, associationContext );

		Map<RowKey, Tuple> tuples = new HashMap<>();

		for ( Map<String, Object> relationship : relationships ) {
			OrientDBTupleAssociationSnapshot snapshot = new OrientDBTupleAssociationSnapshot( relationship, associationKey,
					associationContext );
			RowKey rowKey = convert( associationKey, snapshot );
			tuples.put( rowKey, new Tuple( snapshot ) );
		}
		return tuples;
	}

	private RowKey convert(AssociationKey associationKey, OrientDBTupleAssociationSnapshot snapshot) {
		String[] columnNames = associationKey.getMetadata().getRowKeyColumnNames();
		Object[] values = new Object[columnNames.length];
		for ( int i = 0; i < columnNames.length; i++ ) {
			values[i] = snapshot.get( columnNames[i] );
			log.debugf( "convert: columnName: %s ; value: %s ", columnNames[i], values[i] );
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
				createRelationship( associationKey, action.getValue(), associatedEntityKeyMetadata );
			}
			else {
				log.debugf( "putAssociationOperation: :  associations for  metadata: %s is %d", associationKey.getMetadata(), relationship.size() );
				// relationship = createRelationship( associationKey, action.getValue(), associatedEntityKeyMetadata );
				// throw new UnsupportedOperationException("putAssociationOperation: relations not empty not
				// supported!");
			}
		}
		else {
			log.debugf( "putAssociationOperation: no associations for  metadata: %s", associationKey.getMetadata() );
		}

	}

	private void createRelationship(AssociationKey associationKey, Tuple associationRow, AssociatedEntityKeyMetadata associatedEntityKeyMetadata) {
		log.debugf( "createRelationship: associationKey.getMetadata(): %s ; associationRow: %s", associationKey.getMetadata(), associationRow );
		log.debugf( "createRelationship: getAssociationKind: %s", associationKey.getMetadata().getAssociationKind() );
		log.debugf( "createRelationship: getAssociationType:%s", associationKey.getMetadata().getAssociationType() );
		switch ( associationKey.getMetadata().getAssociationKind() ) {
			case EMBEDDED_COLLECTION:
				log.debug( "createRelationship:EMBEDDED_COLLECTION" );
				createRelationshipWithEmbeddedNode( associationKey, associationRow, associatedEntityKeyMetadata );
				break;
			case ASSOCIATION:
				log.debug( "createRelationship:ASSOCIATION" );
				createRelationshipWithEntityNode( associationKey, associationRow, associatedEntityKeyMetadata );
				break;
			default:
				throw new AssertionFailure( "Unrecognized associationKind: " + associationKey.getMetadata().getAssociationKind() );
		}
	}

	private void createRelationshipWithEntityNode(AssociationKey associationKey, Tuple associationRow,
			AssociatedEntityKeyMetadata associatedEntityKeyMetadata) {
		log.debugf( "createRelationshipWithEntityNode: associationKey.getMetadata(): %s ; associationRow: %s ; associatedEntityKeyMetadata: %s",
				associationKey.getMetadata(), associationRow, associatedEntityKeyMetadata );
		// @TODO equals with createRelationshipWithEmbeddedNode?
		GenerationResult result = INSERT_QUERY_GENERATOR.generate( associationKey.getTable(), associationRow, false, Collections.<String>emptySet() );
		log.debugf( "createRelationshipWithEntityNode: query: %s", result.getExecutionQuery() );
		try {
			PreparedStatement pstmt = provider.getConnection().prepareStatement( result.getExecutionQuery() );
			QueryUtil.setParameters( pstmt, result.getPreparedStatementParams() );
			log.debugf( "createRelationshipWithEntityNode: execute insert query: %d", pstmt.executeUpdate() );
		}
		catch (SQLException sqle) {
			throw log.cannotExecuteQuery( result.getExecutionQuery(), sqle );
		}
	}

	private void createRelationshipWithEmbeddedNode(AssociationKey associationKey, Tuple associationRow,
			AssociatedEntityKeyMetadata associatedEntityKeyMetadata) {
		log.debugf( "createRelationshipWithEmbeddedNode: associationKey.getMetadata(): %s ; associationRow: %s ; associatedEntityKeyMetadata: %s",
				associationKey.getMetadata(), associationRow, associatedEntityKeyMetadata );
		GenerationResult result = INSERT_QUERY_GENERATOR.generate( associationKey.getTable(), associationRow, false, Collections.<String>emptySet() );
		log.debugf( "createRelationshipWithEmbeddedNode: query: %s", result.getExecutionQuery() );
		try {
			PreparedStatement pstmt = provider.getConnection().prepareStatement( result.getExecutionQuery() );
			QueryUtil.setParameters( pstmt, result.getPreparedStatementParams() );
			log.debugf( "createRelationshipWithEmbeddedNode: execute insert query: %d", pstmt.executeUpdate() );
		}
		catch (SQLException sqle) {
			throw log.cannotExecuteQuery( result.getExecutionQuery(), sqle );
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

	@Override
	public ClosableIterator<Tuple> executeBackendQuery(BackendQuery<String> backendQuery, QueryParameters queryParameters) {

		Map<String, Object> parameters = getNamedParameterValuesConvertedByGridType( queryParameters );
		String nativeQuery = buildNativeQuery( backendQuery, queryParameters );
		try {
			PreparedStatement pstmt = provider.getConnection().prepareStatement( nativeQuery );
			int paramIndex = 1;
			for ( Map.Entry<String, TypedGridValue> entry : queryParameters.getNamedParameters().entrySet() ) {
				String key = entry.getKey();
				TypedGridValue value = entry.getValue();
				log.debugf( "executeBackendQuery: key: %s ; type: %s ; value: %s ", 
                                        key, value.getType().getName(), value.getValue().hashCode() );
				try {
					VALUE_SETTER_MAP.get(value.getType().getName()).setValue(pstmt, paramIndex, value.getValue());
				}
				catch (SQLException sqle) {
					throw log.cannotSetValueForParameter( paramIndex, sqle );
				}
				paramIndex++;
			}
			ResultSet rs = pstmt.executeQuery();
			return new ResultSetTupleIterator( rs );
		}
		catch (SQLException e) {
			throw log.cannotExecuteQuery( nativeQuery, e );
		}
	}

	private String buildNativeQuery(BackendQuery<String> customQuery, QueryParameters queryParameters) {
		return customQuery.getQuery();
	}

	/**
	 * Returns a map with the named parameter values from the given parameters object, converted by the {@link GridType}
	 * corresponding to each parameter type.
	 */
	private Map<String, Object> getNamedParameterValuesConvertedByGridType(QueryParameters queryParameters) {
		Map<String, Object> parameterValues = new HashMap<>( queryParameters.getNamedParameters().size() );
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
	public void injectServices(ServiceRegistryImplementor serviceRegistry) {
		this.serviceRegistry = serviceRegistry;
	}

	@Override
	public void sessionFactoryCreated(SessionFactoryImplementor sessionFactoryImplementor) {
		this.associationQueries = initializeAssociationQueries( sessionFactoryImplementor );
		this.entityQueries = initializeEntityQueries( sessionFactoryImplementor, associationQueries );
	}

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
		// persist calendars as ISO8601 strings, including TZ info
		else if ( type == StandardBasicTypes.CALENDAR ) {
			return Iso8601StringCalendarType.DATE_TIME;
		}
		else if ( type == StandardBasicTypes.CALENDAR_DATE ) {
			return Iso8601StringCalendarType.DATE;
		}
		// persist date as ISO8601 strings, in UTC, without TZ info
		else if ( type == StandardBasicTypes.DATE ) {
			return Iso8601StringDateType.DATE;
		}
		else if ( type == StandardBasicTypes.TIME ) {
			return Iso8601StringDateType.TIME;
		}
		else if ( type == StandardBasicTypes.TIMESTAMP ) {
			return Iso8601StringDateType.DATE_TIME;
		}
		else if ( type instanceof SerializableToBlobType ) {
			SerializableToBlobType<?> exposedType = (SerializableToBlobType<?>) type;
			return new SerializableAsStringType<>( exposedType.getJavaTypeDescriptor() );
		}
		else {
			gridType = super.overrideType( type );
		}
		return gridType;
	}
        
        private static interface ValueSetter<T> {
            void setValue(PreparedStatement preparedStatement,int index, T value ) throws SQLException;
        }
        
        private static class StringValueSetter implements ValueSetter<String> {

        @Override
        public void setValue(PreparedStatement preparedStatement,int index, String value) throws SQLException {
            preparedStatement.setString(index, value);
        }
            
        }
        
        private static class LongValueSetter implements ValueSetter<Long> {

        @Override
        public void setValue(PreparedStatement preparedStatement,int index, Long value) throws SQLException {
            preparedStatement.setLong(index, value);
        }
            
        }
        
        private static class IntegerValueSetter implements ValueSetter<Integer> {

        @Override
        public void setValue(PreparedStatement preparedStatement,int index, Integer value) throws SQLException {
            log.debugf("IntegerValueSetter: value: %s", value);
            preparedStatement.setInt(index, value);
        }
            
        }

}
