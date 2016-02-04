/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.datastore.ogm.orientdb;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.hibernate.datastore.ogm.orientdb.dialect.impl.OrientDBAssociationQueries;
import org.hibernate.datastore.ogm.orientdb.dialect.impl.OrientDBEntityQueries;
import org.hibernate.datastore.ogm.orientdb.dialect.impl.OrientDBTupleSnapshot;
import org.hibernate.datastore.ogm.orientdb.impl.OrientDBDatastoreProvider;
import org.hibernate.datastore.ogm.orientdb.logging.impl.Log;
import org.hibernate.datastore.ogm.orientdb.logging.impl.LoggerFactory;
import org.hibernate.datastore.ogm.orientdb.query.impl.OrientDBParameterMetadataBuilder;
import org.hibernate.datastore.ogm.orientdb.type.spi.ORecordIdGridType;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.ogm.dialect.identity.spi.IdentityColumnAwareGridDialect;
import org.hibernate.ogm.dialect.multiget.spi.MultigetGridDialect;
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
import org.hibernate.ogm.model.key.spi.AssociationKey;
import org.hibernate.ogm.model.key.spi.AssociationKeyMetadata;
import org.hibernate.ogm.model.key.spi.EntityKey;
import org.hibernate.ogm.model.key.spi.EntityKeyMetadata;
import org.hibernate.ogm.model.spi.Association;
import org.hibernate.ogm.model.spi.Tuple;
import org.hibernate.ogm.persister.impl.OgmCollectionPersister;
import org.hibernate.ogm.persister.impl.OgmEntityPersister;
import org.hibernate.ogm.type.spi.GridType;
import org.hibernate.persister.collection.CollectionPersister;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.type.Type;

import com.orientechnologies.orient.core.id.ORecordId;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.hibernate.datastore.ogm.orientdb.dialect.impl.ResultSetTupleIterator;
import org.hibernate.datastore.ogm.orientdb.utils.EntityKeyUtil;
import org.hibernate.ogm.dialect.query.spi.QueryableGridDialect;
import org.hibernate.service.spi.ServiceRegistryAwareService;
import org.hibernate.service.spi.ServiceRegistryImplementor;

/**
 * @author Sergey Chernolyas (sergey.chernolyas@gmail.com)
 */
public class OrientDBDialect extends BaseGridDialect implements MultigetGridDialect, QueryableGridDialect<String>,
		ServiceRegistryAwareService, SessionFactoryLifecycleAwareDialect, IdentityColumnAwareGridDialect {

	private static final Log log = LoggerFactory.getLogger();

	private OrientDBDatastoreProvider provider;
	private ServiceRegistryImplementor serviceRegistry;
	private Map<AssociationKeyMetadata, OrientDBAssociationQueries> associationQueries;
	private Map<EntityKeyMetadata, OrientDBEntityQueries> entityQueries;
	private static final Set<String> SYSTEM_FIELDS;

	static {
		Set<String> set = new HashSet<>();
		set.add( "@rid" );
		set.add( "@version" );
		SYSTEM_FIELDS = Collections.unmodifiableSet( set );
	}

	public OrientDBDialect(OrientDBDatastoreProvider provider) {
		this.provider = provider;
	}

	@Override
	public Tuple getTuple(EntityKey key, TupleContext tupleContext) {
		log.info( "getTuple:EntityKey:" + key + "; tupleContext" + tupleContext );
		Object dbKey = EntityKeyUtil.findPrimaryKeyValue( key );

		if ( dbKey != null ) {
			if ( dbKey instanceof ORecordId ) {
				ORecordId oDbKey = (ORecordId) dbKey;
				if ( oDbKey.getClusterPosition() == ORecordId.EMPTY_RECORD_ID.getClusterPosition() ) {
					// is is temporary value. no entity in db with this key
					log.info( "getTuple:Key:" + dbKey + "is temporary!" );
					return null;
				}
			}
		}
		else {
			log.info( "getTuple: Search Entity by business key (not a @rid)" );
		}

		try {
			Map<String, Object> dbValuesMap = entityQueries.get( key.getMetadata() ).findEntity( provider.getConnection(), key );
			return new Tuple(
					new OrientDBTupleSnapshot( dbValuesMap, tupleContext.getAllAssociatedEntityKeyMetadata(), tupleContext.getAllRoles(), key.getMetadata() ) );
		}
		catch (SQLException e) {
			log.error( "Can not find entity", e );
		}
		return null;
	}

	@Override
	public Tuple createTuple(EntityKey key, TupleContext tupleContext) {
		log.info( "createTuple:EntityKey:" + key + "; tupleContext" + tupleContext );
		return new Tuple( new OrientDBTupleSnapshot( key.getMetadata() ) );
	}

	@Override
	public Tuple createTuple(EntityKeyMetadata entityKeyMetadata, TupleContext tupleContext) {
		log.info( "createTuple:EntityKeyMetadata:" + entityKeyMetadata + "; tupleContext" + tupleContext );
		// throw new UnsupportedOperationException("Not supported yet.");
		return new Tuple( new OrientDBTupleSnapshot( entityKeyMetadata ) );
	}

	@Override
	public void insertOrUpdateTuple(EntityKey key, Tuple tuple, TupleContext tupleContext) throws TupleAlreadyExistsException {
		log.info( "insertOrUpdateTuple:EntityKey:" + key + "; tupleContext" + tupleContext + "; tuple:" + tuple );
		Connection connection = provider.getConnection();

		StringBuilder queryBuffer = new StringBuilder();
		ORecordId dbKey = null;
		// @TODO extract method
		for ( int i = 0; i < key.getColumnNames().length; i++ ) {
			String columnName = key.getColumnNames()[i];
			Object columnValue = key.getColumnValues()[i];
			log.info( "EntityKey: columnName: " + columnName + ";columnValue: " + columnValue + " (class:" + columnValue.getClass().getName() + ");" );
			if ( columnName.equals( "@rid" ) ) {
				dbKey = (ORecordId) columnValue;
			}
		}
		if ( dbKey.getClusterPosition() == ORecordId.EMPTY_RECORD_ID.getClusterPosition() ) {
			// it is new record =>insert a new record!
			log.info( "insertOrUpdateTuple:Key:" + dbKey + "is temporary!. Insert new record" );
			queryBuffer.append( "insert into " ).append( key.getTable() );
			if ( tuple.getColumnNames().size() > 1 ) {
				queryBuffer.append( " set " );
			}
			for ( String columnName : tuple.getColumnNames() ) {
				if ( SYSTEM_FIELDS.contains( columnName ) ) {
					continue;
				}
				// @TODO correct type
				queryBuffer.append( " " ).append( columnName ).append( "='" ).append( tuple.get( columnName ) ).append( "'," );
			}
			queryBuffer.setLength( queryBuffer.length() - 1 );
			log.info( "insertOrUpdateTuple:Key:" + dbKey + ". insert query:" + queryBuffer.toString() );
		}
		else {
			// it is old record =>update the record
			log.info( "insertOrUpdateTuple:Key:" + dbKey + "is persistent! Update old record!" );
			queryBuffer.append( "update  " ).append( dbKey ).append( " set " );
			for ( String columnName : tuple.getColumnNames() ) {
				if ( SYSTEM_FIELDS.contains( columnName ) ) {
					continue;
				}
				// @TODO correct type
				queryBuffer.append( " " ).append( columnName ).append( "='" ).append( tuple.get( columnName ) ).append( "'," );
			}
			queryBuffer.setLength( queryBuffer.length() - 1 );
			log.info( "insertOrUpdateTuple:Key:" + dbKey + ". update query:" + queryBuffer.toString() );
		}

		try {
			PreparedStatement pstmt = connection.prepareStatement( queryBuffer.toString() );
			log.info( "insertOrUpdateTuple:Key:" + dbKey + ". inserted or updated: " + pstmt.executeUpdate() );
		}
		catch (SQLException e) {
			log.error( "Can not find entity", e );
			throw new RuntimeException( e );
		}
	}

	@Override
	public void insertTuple(EntityKeyMetadata entityKeyMetadata, Tuple tuple, TupleContext tupleContext) {
		log.info( "insertTuple:EntityKeyMetadata:" + entityKeyMetadata + "; tupleContext" + tupleContext + "; tuple:" + tuple );

		StringBuilder insertQuery = new StringBuilder( 100 );
		insertQuery.append( "insert into " ).append( entityKeyMetadata.getTable() ).append( " " );
		if ( !tuple.getColumnNames().isEmpty() ) {
			insertQuery.append( " set " );
		}

		String dbKeyName = entityKeyMetadata.getColumnNames()[0];
		Long dbKeyValue = null;
		Connection connection = provider.getConnection();
		boolean useBusinessKey = false;
		if ( dbKeyName.equals( "@rid" ) ) {
			// use @RID for key
		}
		else {
			// use business key. get new id from sequence
			useBusinessKey = true;
			StringBuilder seqName = new StringBuilder( "seq_" );
			seqName.append( entityKeyMetadata.getTable().toLowerCase() ).append( "_" );
			seqName.append( dbKeyName.toLowerCase() );
			log.info( "insertTuple:seq name :" + seqName );
			try {
				Statement stmt = connection.createStatement();
				ResultSet rs = stmt.executeQuery( "select sequence('" + seqName.toString() + "').next()" );
				if ( rs.next() ) {
					dbKeyValue = rs.getLong( "sequence" );
					tuple.put( dbKeyName, dbKeyValue );
				}
				log.info( "insertTuple:dbKeyValue :" + dbKeyValue );
			}
			catch (SQLException e) {
				log.error( "Can not insert entity", e );
				throw new RuntimeException( e );
			}
		}

		for ( String columnName : tuple.getColumnNames() ) {
			Object value = tuple.get( columnName );
			if ( SYSTEM_FIELDS.contains( columnName ) ) {
				continue;
			}
			log.info( "insertTuple:columnName:" + columnName + "; value:" + value );
			insertQuery.append( columnName ).append( "=" );

			if ( value instanceof String ) {
				insertQuery.append( "'" ).append( value ).append( "'" );
			}
			else {
				insertQuery.append( value );
			}
			insertQuery.append( "," );
		}
		insertQuery.setLength( insertQuery.length() - 1 );
		log.info( "insertTuple: insertQuery: " + insertQuery.toString() );

		try {
			PreparedStatement pstmt = connection.prepareStatement( insertQuery.toString() );
			log.info( "insertTuple: insert: " + pstmt.executeUpdate() );
		}
		catch (SQLException e) {
			log.error( "Can not insert entity", e );
			throw new RuntimeException( e );
		}
	}

	@Override
	public void removeTuple(EntityKey key, TupleContext tupleContext) {
		log.info( "removeTuple:EntityKey:" + key + "; tupleContext" + tupleContext );
		Connection connection = provider.getConnection();
		StringBuilder queryBuffer = new StringBuilder();
		ORecordId dbKey = null;
		for ( int i = 0; i < key.getColumnNames().length; i++ ) {
			String columnName = key.getColumnNames()[i];
			Object columnValue = key.getColumnValues()[i];
			log.info( "EntityKey: columnName: " + columnName + ";columnValue: " + columnValue + " (class:" + columnValue.getClass().getName() + ");" );
			if ( columnName.equals( "@rid" ) ) {
				dbKey = (ORecordId) columnValue;
			}
		}

		try {
			queryBuffer.append( "DELETE VERTEX " ).append( dbKey );
			PreparedStatement pstmt = connection.prepareStatement( queryBuffer.toString() );
			log.info( "removeTuple:Key:" + dbKey + ". remove: " + pstmt.executeUpdate() );
		}
		catch (SQLException e) {
			log.error( "Can not find entity", e );
			throw new RuntimeException( e );
		}
	}

	@Override
	public Association getAssociation(AssociationKey key, AssociationContext associationContext) {
		throw new UnsupportedOperationException( "Not supported yet." );
	}

	@Override
	public Association createAssociation(AssociationKey key, AssociationContext associationContext) {
		throw new UnsupportedOperationException( "Not supported yet." );
	}

	@Override
	public void insertOrUpdateAssociation(AssociationKey key, Association association, AssociationContext associationContext) {
		throw new UnsupportedOperationException( "Not supported yet." );
	}

	@Override
	public void removeAssociation(AssociationKey key, AssociationContext associationContext) {
		throw new UnsupportedOperationException( "Not supported yet." );
	}

	@Override
	public boolean isStoredInEntityStructure(AssociationKeyMetadata associationKeyMetadata, AssociationTypeContext associationTypeContext) {
		return true;
	}

	@Override
	public Number nextValue(NextValueRequest request) {
		log.info( "NextValueRequest:" + request + "; " );
		// return ORecordId.EMPTY_RECORD_ID.getClusterPosition();
		throw new UnsupportedOperationException( "Not supported yet." );
	}

	@Override
	public boolean supportsSequences() {
		return super.supportsSequences(); // To change body of generated methods, choose Tools | Templates.
	}

	@Override
	public void forEachTuple(ModelConsumer consumer, EntityKeyMetadata... entityKeyMetadatas) {
		throw new UnsupportedOperationException( "Not supported yet." );
	}

	@Override
	public List<Tuple> getTuples(EntityKey[] keys, TupleContext tupleContext) {
		throw new UnsupportedOperationException( "Not supported yet." ); // To change body of generated methods, choose
																			// Tools | Templates.
	}

	@Override
	public ClosableIterator<Tuple> executeBackendQuery(BackendQuery<String> backendQuery, QueryParameters queryParameters) {

		Map<String, Object> parameters = getNamedParameterValuesConvertedByGridType( queryParameters );
		String nativeQuery = buildNativeQuery( backendQuery, queryParameters );
		try {
			log.info( "3.executeBackendQuery.native query: " + nativeQuery );
			PreparedStatement pstmt = provider.getConnection().prepareStatement( nativeQuery );
			for ( Map.Entry<String, TypedGridValue> entry : queryParameters.getNamedParameters().entrySet() ) {
				String key = entry.getKey();
				TypedGridValue value = entry.getValue();
				log.info( "key: " + key + "; type:" + value.getType().getName() + "; value:" + value.getValue() );
				// @todo move to Map
				if ( value.getType().getName().equals( "string" ) ) {
					pstmt.setString( 1, (String) value.getValue() );
				}

			}
			log.info( "3.executeBackendQuery. before pstmt.executeQuery()" );
			ResultSet rs = pstmt.executeQuery();
			log.info( "3.executeBackendQuery. after pstmt.executeQuery()" );
			/*
			 * if (backendQuery.getSingleEntityKeyMetadataOrNull() != null) { return new NodesTupleIterator(result,
			 * backendQuery.getSingleEntityKeyMetadataOrNull()); }
			 */
			return new ResultSetTupleIterator( rs );
		}
		catch (SQLException e) {
			log.error( "Error with ResultSet", e );
			throw new RuntimeException( e );
		}
	}

	private String buildNativeQuery(BackendQuery<String> customQuery, QueryParameters queryParameters) {
		StringBuilder nativeQuery = new StringBuilder( customQuery.getQuery() );
		log.info( "2.buildNativeQuery.native query: " + customQuery.getQuery() );
		return nativeQuery.toString();
	}

	/**
	 * Returns a map with the named parameter values from the given parameters object, converted by the {@link GridType}
	 * corresponding to each parameter type.
	 */
	private Map<String, Object> getNamedParameterValuesConvertedByGridType(QueryParameters queryParameters) {
		log.info( "getNamedParameterValuesConvertedByGridType. named parameters: " + queryParameters.getNamedParameters().size() );
		Map<String, Object> parameterValues = new HashMap<String, Object>( queryParameters.getNamedParameters().size() );
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
		log.info( "1.parseNativeQuery.native query: " + nativeQuery );
		// We return given native SQL query as they is; Currently there is no API for validating OrientDB queries
		// without
		// actually executing them
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
		Map<EntityKeyMetadata, OrientDBEntityQueries> queryMap = new HashMap<EntityKeyMetadata, OrientDBEntityQueries>();
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
		Map<AssociationKeyMetadata, OrientDBAssociationQueries> queryMap = new HashMap<AssociationKeyMetadata, OrientDBAssociationQueries>();
		Collection<CollectionPersister> collectionPersisters = sessionFactoryImplementor.getCollectionPersisters().values();
		for ( CollectionPersister collectionPersister : collectionPersisters ) {
			if ( collectionPersister instanceof OgmCollectionPersister ) {
				OgmCollectionPersister ogmCollectionPersister = (OgmCollectionPersister) collectionPersister;
				EntityKeyMetadata ownerEntityKeyMetadata = ( (OgmEntityPersister) ( ogmCollectionPersister.getOwnerEntityPersister() ) ).getEntityKeyMetadata();
				AssociationKeyMetadata associationKeyMetadata = ogmCollectionPersister.getAssociationKeyMetadata();
				queryMap.put( associationKeyMetadata, new OrientDBAssociationQueries( ownerEntityKeyMetadata, associationKeyMetadata ) );
			}
		}
		return queryMap;
	}

	@Override
	public GridType overrideType(Type type) {
		log.info( "overrideType:" + type.getName() + ";" + type.getReturnedClass() );
		GridType gridType = null;
		if ( type.getName().equals( "com.orientechnologies.orient.core.id.ORecordId" ) ) {
			gridType = ORecordIdGridType.INSTANCE;
		}
		else {
			gridType = super.overrideType( type ); // To change body of generated methods, choose Tools | Templates.
		}
		return gridType;
	}

}
