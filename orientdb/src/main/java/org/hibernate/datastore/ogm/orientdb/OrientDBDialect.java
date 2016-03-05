/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.datastore.ogm.orientdb;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

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
<<<<<<< HEAD
import org.hibernate.ogm.dialect.multiget.spi.MultigetGridDialect;
=======
>>>>>>> 3712b2f73e6a708158478452211328c54279a26c
import org.hibernate.ogm.dialect.query.spi.BackendQuery;
import org.hibernate.ogm.dialect.query.spi.ClosableIterator;
import org.hibernate.ogm.dialect.query.spi.ParameterMetadataBuilder;
import org.hibernate.ogm.dialect.query.spi.QueryParameters;
<<<<<<< HEAD
=======
import org.hibernate.ogm.dialect.query.spi.QueryableGridDialect;
>>>>>>> 3712b2f73e6a708158478452211328c54279a26c
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
import org.hibernate.ogm.model.key.spi.IdSourceKeyMetadata.IdSourceType;
import org.hibernate.ogm.model.key.spi.RowKey;
import org.hibernate.ogm.model.spi.Association;
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

<<<<<<< HEAD
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.hibernate.datastore.ogm.orientdb.constant.OrientDBConstant;
import org.hibernate.datastore.ogm.orientdb.dialect.impl.OrientDBAssociationSnapshot;
import org.hibernate.datastore.ogm.orientdb.dialect.impl.OrientDBTupleAssociationSnapshot;
import org.hibernate.datastore.ogm.orientdb.dialect.impl.ResultSetTupleIterator;
import org.hibernate.datastore.ogm.orientdb.impl.OrientDBSchemaDefiner;
import org.hibernate.datastore.ogm.orientdb.utils.EntityKeyUtil;
import org.hibernate.ogm.dialect.query.spi.QueryableGridDialect;
import org.hibernate.ogm.model.key.spi.RowKey;
import org.hibernate.service.spi.ServiceRegistryAwareService;
import org.hibernate.service.spi.ServiceRegistryImplementor;
=======
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.id.ORecordId;
>>>>>>> 3712b2f73e6a708158478452211328c54279a26c

/**
 * @author Sergey Chernolyas (sergey.chernolyas@gmail.com)
 */
<<<<<<< HEAD
public class OrientDBDialect extends BaseGridDialect implements MultigetGridDialect, QueryableGridDialect<String>,
		ServiceRegistryAwareService, SessionFactoryLifecycleAwareDialect, IdentityColumnAwareGridDialect {
=======
public class OrientDBDialect extends BaseGridDialect implements QueryableGridDialect<String>,
ServiceRegistryAwareService, SessionFactoryLifecycleAwareDialect, IdentityColumnAwareGridDialect {
>>>>>>> 3712b2f73e6a708158478452211328c54279a26c

	private static final long serialVersionUID = 1L;
	private static final Log log = LoggerFactory.getLogger();
	private static final Association ASSOCIATION_NOT_FOUND = null;

	private OrientDBDatastoreProvider provider;
	private ServiceRegistryImplementor serviceRegistry;
	private Map<AssociationKeyMetadata, OrientDBAssociationQueries> associationQueries;
	private Map<EntityKeyMetadata, OrientDBEntityQueries> entityQueries;

	public OrientDBDialect(OrientDBDatastoreProvider provider) {
		this.provider = provider;
	}

	@Override
	public Tuple getTuple(EntityKey key, TupleContext tupleContext) {
<<<<<<< HEAD
		log.info( "getTuple:EntityKey:" + key + "; tupleContext" + tupleContext + " tupleContext.getClass():" + tupleContext.getClass() );

		try {
			if ( !entityQueries.containsKey( key.getMetadata() ) ) {
				// throw new EntityNotFoundException("Key : "+key.toString());
				return null;
			}
			Map<String, Object> dbValuesMap = entityQueries.get( key.getMetadata() ).findEntity( provider.getConnection(), key );
			if ( dbValuesMap == null || ( dbValuesMap != null && dbValuesMap.isEmpty() ) ) {
				return null;
			}
			return new Tuple(
					new OrientDBTupleSnapshot( dbValuesMap, tupleContext.getAllAssociatedEntityKeyMetadata(), tupleContext.getAllRoles(), key.getMetadata() ) );
		}
		catch (SQLException e) {
			log.error( "Can not find entity", e );
		}
		return null;
=======
		log.debug( "getTuple:EntityKey:" + key + "; tupleContext" + tupleContext + " tupleContext.getClass():" + tupleContext.getClass() );
		Map<String, Object> dbValuesMap = entityQueries.get( key.getMetadata() ).findEntity( provider.getConnection(), key );
		if ( dbValuesMap == null || dbValuesMap.isEmpty() ) {
			return null;
		}
		return new Tuple(
				new OrientDBTupleSnapshot( dbValuesMap, tupleContext.getAllAssociatedEntityKeyMetadata(), tupleContext.getAllRoles(), key.getMetadata() ) );

	}

	@Override
	public void forEachTuple(ModelConsumer consumer, EntityKeyMetadata... entityKeyMetadatas) {
		throw new UnsupportedOperationException( "forEachTuple!.Not supported yet." );
>>>>>>> 3712b2f73e6a708158478452211328c54279a26c
	}

	@Override
	public void forEachTuple(ModelConsumer consumer, EntityKeyMetadata... entityKeyMetadatas) {
		throw new UnsupportedOperationException( "forEachTuple!.Not supported yet." );
	}

	@Override
	public List<Tuple> getTuples(EntityKey[] keys, TupleContext tupleContext) {
		ArrayList<Tuple> tuples = new ArrayList<>( keys.length );
		for ( EntityKey key : keys ) {
			log.info( "getTuples:EntityKey:" + key + "; tupleContext" + tupleContext );
			tuples.add( getTuple( key, tupleContext ) );
		}
		return tuples;
	}

	@Override
	public Tuple createTuple(EntityKey key, TupleContext tupleContext) {
<<<<<<< HEAD
		log.info( "createTuple:EntityKey:" + key + "; tupleContext" + tupleContext + "; tupleContext.getClass():" + tupleContext.getClass() );
=======
		log.debug( "createTuple:EntityKey:" + key + "; tupleContext" + tupleContext + "; tupleContext.getClass():" + tupleContext.getClass() );
>>>>>>> 3712b2f73e6a708158478452211328c54279a26c
		return new Tuple( new OrientDBTupleSnapshot( tupleContext.getAllAssociatedEntityKeyMetadata(), tupleContext.getAllRoles(), key.getMetadata() ) );
	}

	@Override
	public Tuple createTuple(EntityKeyMetadata entityKeyMetadata, TupleContext tupleContext) {
<<<<<<< HEAD
		log.info( "createTuple:EntityKeyMetadata:" + entityKeyMetadata + "; tupleContext" + tupleContext + ";tupleContext.getClass():"
				+ tupleContext.getClass() );
		return new Tuple( new OrientDBTupleSnapshot( tupleContext.getAllAssociatedEntityKeyMetadata(), tupleContext.getAllRoles(), entityKeyMetadata ) );
=======

		log.debug( "createTuple:EntityKeyMetadata:" + entityKeyMetadata + "; tupleContext" + tupleContext + ";tupleContext.getClass():"
				+ tupleContext.getClass() );
		return new Tuple( new OrientDBTupleSnapshot( tupleContext.getAllAssociatedEntityKeyMetadata(), tupleContext.getAllRoles(), entityKeyMetadata ) );
	}

	/**
	 * util method for settings Tuple's keys to query. If primaryKeyName has value then all columns will add to buffer
	 * except primaryKeyColumn
	 *
	 * @param queryBuffer buffer for query
	 * @param tuple tuple
	 * @param primaryKeyName primary key column name
	 * @return list of query parameters
	 */

	private List<Object> addTupleFields(StringBuilder queryBuffer, Tuple tuple, String primaryKeyName, boolean forInsert) {
		LinkedList<Object> preparedStatementParams = new LinkedList<>();
		for ( String columnName : tuple.getColumnNames() ) {
			if ( OrientDBConstant.SYSTEM_FIELDS.contains( columnName ) ||
					( primaryKeyName != null && columnName.equals( primaryKeyName ) ) ) {
				continue;
			}
			log.debug( "addTupleFields: Set value for column " + columnName );
			if ( !forInsert ) {
				queryBuffer.append( columnName ).append( "=" );
			}
			if ( tuple.get( columnName ) instanceof byte[] ) {
				queryBuffer.append( "?" );
				preparedStatementParams.add( tuple.get( columnName ) );
			}
			else if ( tuple.get( columnName ) instanceof BigInteger ) {
				queryBuffer.append( "?" );
				BigInteger bi = (BigInteger) tuple.get( columnName );
				preparedStatementParams.add( bi.toByteArray() );
			}
			else if ( tuple.get( columnName ) instanceof BigDecimal ) {
				queryBuffer.append( "?" );
				preparedStatementParams.add( tuple.get( columnName ) );
			}
			else {
				EntityKeyUtil.setFieldValue( queryBuffer, tuple.get( columnName ) );
			}
			queryBuffer.append( " ," );
		}
		if ( forInsert ) {
			queryBuffer.setLength( queryBuffer.length() - 1 );
		}

		return preparedStatementParams;
>>>>>>> 3712b2f73e6a708158478452211328c54279a26c
	}

	@Override
	public void insertOrUpdateTuple(EntityKey key, Tuple tuple, TupleContext tupleContext) throws TupleAlreadyExistsException {

		log.debug( "insertOrUpdateTuple:EntityKey:" + key + "; tupleContext" + tupleContext + "; tuple:" + tuple );
		Connection connection = provider.getConnection();

		StringBuilder queryBuffer = new StringBuilder();
		String dbKeyName = key.getColumnNames()[0];
		Object dbKeyValue = key.getColumnValues()[0];

		for ( int i = 0; i < key.getColumnNames().length; i++ ) {
			String columnName = key.getColumnNames()[i];
			Object columnValue = key.getColumnValues()[i];
<<<<<<< HEAD
			log.info( "EntityKey: columnName: " + columnName + ";columnValue: " + columnValue + " (class:" + columnValue.getClass().getName() + ");" );
		}
		try {
			boolean existsPrimaryKey = EntityKeyUtil.existsPrimaryKeyInDB( provider.getConnection(), key );
			log.info( "insertOrUpdateTuple:Key:" + dbKeyName + " exists in database ? " + existsPrimaryKey );

			if ( existsPrimaryKey ) {
				// it is update
				queryBuffer.append( "update  " ).append( key.getTable() ).append( "  set " );

				for ( String columnName : tuple.getColumnNames() ) {
					if ( OrientDBConstant.SYSTEM_FIELDS.contains( columnName ) || columnName.equals( dbKeyName ) ) {
						continue;
					}
					// @TODO correct type
					queryBuffer.append( " " ).append( columnName ).append( "=" );
					EntityKeyUtil.setFieldValue( queryBuffer, tuple.get( columnName ) );
					queryBuffer.append( "," );
				}
				queryBuffer.setLength( queryBuffer.length() - 1 );
				queryBuffer.append( " WHERE " ).append( dbKeyName ).append( "=" );
				EntityKeyUtil.setFieldValue( queryBuffer, dbKeyValue );
			}
			else {
				// it is insert with business key which set already
				log.info( "insertOrUpdateTuple:Key:" + dbKeyName + " is new! Insert new record!" );
				queryBuffer.append( "insert into " ).append( key.getTable() ).append( "  set " );
				for ( String columnName : tuple.getColumnNames() ) {
					if ( OrientDBConstant.SYSTEM_FIELDS.contains( columnName ) ) {
						continue;
					}
					// @TODO correct type
					queryBuffer.append( " " ).append( columnName ).append( "=" );
					EntityKeyUtil.setFieldValue( queryBuffer, tuple.get( columnName ) );
					queryBuffer.append( "," );
				}
				queryBuffer.append( dbKeyName ).append( " = " );
				EntityKeyUtil.setFieldValue( queryBuffer, dbKeyValue );
			}

			log.info( "insertOrUpdateTuple:Key:" + dbKeyName + " (" + dbKeyValue + ").  query:" + queryBuffer.toString() );
			PreparedStatement pstmt = connection.prepareStatement( queryBuffer.toString() );
			log.info( "insertOrUpdateTuple:Key:" + dbKeyName + " (" + dbKeyValue + "). inserted or updated: " + pstmt.executeUpdate() );
		}
		catch (SQLException e) {
			log.error( "Can not find entity", e );
			throw new RuntimeException( e );
=======
			log.debug( "EntityKey: columnName: " + columnName + ";columnValue: " + columnValue + " (class:" + columnValue.getClass().getName() + ");" );
		}
		boolean existsPrimaryKey = EntityKeyUtil.existsPrimaryKeyInDB( provider.getConnection(), key );
		log.debug( "insertOrUpdateTuple:Key:" + dbKeyName + " exists in database ? " + existsPrimaryKey );
		List<Object> preparedStatementParams = Collections.emptyList();

		if ( existsPrimaryKey ) {
			// it is update
			queryBuffer.append( "update " ).append( key.getTable() ).append( "  set " );
			preparedStatementParams = addTupleFields( queryBuffer, tuple, dbKeyName, false );
			if ( queryBuffer.toString().endsWith( "," ) ) {
				queryBuffer.setLength( queryBuffer.length() - 1 );
			}
			queryBuffer.append( " WHERE " ).append( dbKeyName ).append( "=" );
			EntityKeyUtil.setFieldValue( queryBuffer, dbKeyValue );
		}
		else {
			// it is insert with business key which set already

			log.debug( "insertOrUpdateTuple:Key:" + dbKeyName + " is new! Insert new record!" );
			queryBuffer.append( "insert into " ).append( key.getTable() ).append( "  (" );
			for ( String columnName : tuple.getColumnNames() ) {
				if ( OrientDBConstant.SYSTEM_FIELDS.contains( columnName ) ) {
					continue;
				}
				queryBuffer.append( columnName ).append( "," );
			}
			queryBuffer.setLength( queryBuffer.length() - 1 );
			queryBuffer.append( ") values (" );
			preparedStatementParams = addTupleFields( queryBuffer, tuple, null, true );
			queryBuffer.append( ")" );
		}
		try {
			log.debug( "insertOrUpdateTuple:Key:" + dbKeyName + " (" + dbKeyValue + "). Query: " + queryBuffer.toString() );
			PreparedStatement pstmt = connection.prepareStatement( queryBuffer.toString() );
			log.debug( "insertOrUpdateTuple: exist parameters for preparedstatement :" + preparedStatementParams.size() );
			setParameters( pstmt, preparedStatementParams );

			log.debug( "insertOrUpdateTuple:Key:" + dbKeyName + " (" + dbKeyValue + "). inserted or updated: " + pstmt.executeUpdate() );
		}
		catch (SQLException sqle) {
			throw log.cannotExecuteQuery( queryBuffer.toString(), sqle );
>>>>>>> 3712b2f73e6a708158478452211328c54279a26c
		}
	}

	@Override
	public void insertTuple(EntityKeyMetadata entityKeyMetadata, Tuple tuple, TupleContext tupleContext) {
<<<<<<< HEAD
		log.info( "insertTuple:EntityKeyMetadata:" + entityKeyMetadata + "; tupleContext" + tupleContext + "; tuple:" + tuple );

		StringBuilder insertQuery = new StringBuilder( 100 );
		insertQuery.append( "insert into " ).append( entityKeyMetadata.getTable() ).append( " " );
		if ( !tuple.getColumnNames().isEmpty() ) {
			insertQuery.append( " set " );
		}
=======
		log.debug( "insertTuple:EntityKeyMetadata:" + entityKeyMetadata + "; tupleContext" + tupleContext + "; tuple:" + tuple );

		StringBuilder insertQuery = new StringBuilder( 100 );
		insertQuery.append( "insert into " ).append( entityKeyMetadata.getTable() ).append( "( " );
>>>>>>> 3712b2f73e6a708158478452211328c54279a26c

		String dbKeyName = entityKeyMetadata.getColumnNames()[0];
		Long dbKeyValue = null;
		Connection connection = provider.getConnection();
<<<<<<< HEAD
=======
		List<Object> preparedStatementParams = Collections.emptyList();
>>>>>>> 3712b2f73e6a708158478452211328c54279a26c

		if ( dbKeyName.equals( OrientDBConstant.SYSTEM_RID ) ) {
			// use @RID for key
			throw new UnsupportedOperationException( "Can not use @RID as primary key!" );
		}
		else {
			// use business key. get new id from sequence
<<<<<<< HEAD

			String seqName = OrientDBSchemaDefiner.generateSeqName( entityKeyMetadata.getTable(), dbKeyName );
			log.info( "insertTuple:seq name :" + seqName );
			try {
				Statement stmt = connection.createStatement();
				ResultSet rs = stmt.executeQuery( "select sequence('" + seqName + "').next()" );
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
			if ( OrientDBConstant.SYSTEM_FIELDS.contains( columnName ) ) {
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
=======
			String seqName = OrientDBSchemaDefiner.generateSeqName( entityKeyMetadata.getTable(), dbKeyName );
			dbKeyValue = (Long) SequenceUtil.getSequence( connection, seqName );
			tuple.put( dbKeyName, dbKeyValue );
		}
		for ( String columnName : tuple.getColumnNames() ) {
			if ( OrientDBConstant.SYSTEM_FIELDS.contains( columnName ) ) {
				continue;
			}
			insertQuery.append( columnName ).append( "," );
		}
		insertQuery.setLength( insertQuery.length() - 1 );
		insertQuery.append( ") values (" );

		preparedStatementParams = addTupleFields( insertQuery, tuple, null, true );
		insertQuery.append( ")" );

		log.debug( "insertTuple: insertQuery: " + insertQuery.toString() );
		try {
			PreparedStatement pstmt = connection.prepareStatement( insertQuery.toString() );
			if ( preparedStatementParams != null ) {
				setParameters( pstmt, preparedStatementParams );
			}
			log.debug( "insertTuple:Key:" + dbKeyName + " (" + dbKeyValue + "). inserted or updated: " + pstmt.executeUpdate() );
		}
		catch (SQLException sqle) {
			throw log.cannotExecuteQuery( insertQuery.toString(), sqle );
		}
	}

	private void setParameters(PreparedStatement pstmt, List<Object> preparedStatementParams) {
		for ( int i = 0; i < preparedStatementParams.size(); i++ ) {
			Object value = preparedStatementParams.get( i );
			try {
				if ( value instanceof byte[] ) {
					pstmt.setBytes( i + 1, (byte[]) value );
				}
				else {
					pstmt.setObject( i + 1, value );
				}
			}
			catch (SQLException sqle) {
				throw log.cannotSetValueForParameter( i + 1, sqle );
			}
>>>>>>> 3712b2f73e6a708158478452211328c54279a26c
		}
	}

	@Override
	public void removeTuple(EntityKey key, TupleContext tupleContext) {
<<<<<<< HEAD
		log.info( "removeTuple:EntityKey:" + key + "; tupleContext" + tupleContext );

=======
		log.debug( "removeTuple:EntityKey:" + key + "; tupleContext" + tupleContext );
>>>>>>> 3712b2f73e6a708158478452211328c54279a26c
		Connection connection = provider.getConnection();
		StringBuilder queryBuffer = new StringBuilder();
		String dbKeyName = EntityKeyUtil.findPrimaryKeyName( key );
		Object dbKeyValue = EntityKeyUtil.findPrimaryKeyValue( key );
		for ( int i = 0; i < key.getColumnNames().length; i++ ) {
			String columnName = key.getColumnNames()[i];
			Object columnValue = key.getColumnValues()[i];
<<<<<<< HEAD
			log.info( "EntityKey: columnName: " + columnName + ";columnValue: " + columnValue + " (class:" + columnValue.getClass().getName() + ");" );
=======
			log.debug( "EntityKey: columnName: " + columnName + ";columnValue: " + columnValue + " (class:" + columnValue.getClass().getName() + ");" );
>>>>>>> 3712b2f73e6a708158478452211328c54279a26c
		}

		try {
			queryBuffer.append( "DELETE VERTEX " ).append( key.getTable() ).append( " where " ).append( dbKeyName ).append( " = " );
			EntityKeyUtil.setFieldValue( queryBuffer, dbKeyValue );
<<<<<<< HEAD
			log.info( "removeTuple:Key:" + dbKeyName + " (" + dbKeyValue + "). query: " + queryBuffer );
			PreparedStatement pstmt = connection.prepareStatement( queryBuffer.toString() );
			log.info( "removeTuple:Key:" + dbKeyName + " (" + dbKeyValue + "). remove: " + pstmt.executeUpdate() );
			entityQueries.remove( key.getMetadata() );
=======
			log.debug( "removeTuple:Key:" + dbKeyName + " (" + dbKeyValue + "). query: " + queryBuffer );
			PreparedStatement pstmt = connection.prepareStatement( queryBuffer.toString() );
			log.debug( "removeTuple:Key:" + dbKeyName + " (" + dbKeyValue + "). remove: " + pstmt.executeUpdate() );
>>>>>>> 3712b2f73e6a708158478452211328c54279a26c
		}
		catch (SQLException e) {
			log.error( "Can not remove entity", e );
			throw new RuntimeException( e );
		}
	}

	@Override
	public Association getAssociation(AssociationKey associationKey, AssociationContext associationContext) {
<<<<<<< HEAD
		log.info( "getAssociation:AssociationKey:" + associationKey + "; AssociationContext" + associationContext );

		try {
			EntityKey entityKey = associationKey.getEntityKey();
			boolean existsPrimaryKey = EntityKeyUtil.existsPrimaryKeyInDB( provider.getConnection(), entityKey );
			if ( !existsPrimaryKey ) {
				// Entity now extists
				return ASSOCIATION_NOT_FOUND;
			}
			Map<RowKey, Tuple> tuples = createAssociationMap( associationKey, associationContext );
			return new Association( new OrientDBAssociationSnapshot( tuples ) );
		}

		catch (SQLException e) {
			log.error( "Can not get association!", e );
		}
		return ASSOCIATION_NOT_FOUND;
	}

	private Map<RowKey, Tuple> createAssociationMap(AssociationKey associationKey, AssociationContext associationContext) throws SQLException {

		List<Map<String, Object>> relationships = entityQueries.get( associationKey.getEntityKey().getMetadata() )
				.findAssociation( provider.getConnection(), associationKey, associationContext );

		Map<RowKey, Tuple> tuples = new HashMap<RowKey, Tuple>();

		for ( Map<String, Object> relationship : relationships ) {
			OrientDBTupleAssociationSnapshot snapshot = new OrientDBTupleAssociationSnapshot( relationship, associationKey, associationContext );
			RowKey rowKey = convert( associationKey, snapshot );
			tuples.put( rowKey, new Tuple( snapshot ) );

=======
		log.debug( "getAssociation:AssociationKey:" + associationKey + "; AssociationContext" + associationContext );
		EntityKey entityKey = associationKey.getEntityKey();
		boolean existsPrimaryKey = EntityKeyUtil.existsPrimaryKeyInDB( provider.getConnection(), entityKey );
		if ( !existsPrimaryKey ) {
			// Entity now extists
			return ASSOCIATION_NOT_FOUND;
		}
		Map<RowKey, Tuple> tuples = createAssociationMap( associationKey, associationContext );
		return new Association( new OrientDBAssociationSnapshot( tuples ) );
	}

	private Map<RowKey, Tuple> createAssociationMap(AssociationKey associationKey, AssociationContext associationContext) {
		List<Map<String, Object>> relationships = entityQueries.get( associationKey.getEntityKey().getMetadata() )
				.findAssociation( provider.getConnection(), associationKey, associationContext );

		Map<RowKey, Tuple> tuples = new HashMap<>();

		for ( Map<String, Object> relationship : relationships ) {
			OrientDBTupleAssociationSnapshot snapshot = new OrientDBTupleAssociationSnapshot( relationship, associationKey,
					associationContext );
			RowKey rowKey = convert( associationKey, snapshot );
			tuples.put( rowKey, new Tuple( snapshot ) );
>>>>>>> 3712b2f73e6a708158478452211328c54279a26c
		}
		return tuples;
	}

	private RowKey convert(AssociationKey associationKey, OrientDBTupleAssociationSnapshot snapshot) {
		String[] columnNames = associationKey.getMetadata().getRowKeyColumnNames();
		Object[] values = new Object[columnNames.length];
<<<<<<< HEAD
		log.info( "convert: columnNames:" + columnNames.length );

		for ( int i = 0; i < columnNames.length; i++ ) {
			values[i] = snapshot.get( columnNames[i] );
			log.info( "convert: columnName:" + columnNames[i] + "; value" + values[i] );
=======
		log.debug( "convert: columnNames:" + columnNames.length );

		for ( int i = 0; i < columnNames.length; i++ ) {
			values[i] = snapshot.get( columnNames[i] );
			log.debug( "convert: columnName:" + columnNames[i] + "; value:" + values[i] );
>>>>>>> 3712b2f73e6a708158478452211328c54279a26c
		}
		return new RowKey( columnNames, values );
	}

	@Override
	public Association createAssociation(AssociationKey key, AssociationContext associationContext) {
<<<<<<< HEAD
		log.info( "createAssociation: AssociationKey:" + key + "; AssociationContext" + associationContext );
=======

		log.debug( "createAssociation: AssociationKey:" + key + "; AssociationContext" + associationContext );
>>>>>>> 3712b2f73e6a708158478452211328c54279a26c
		return new Association();
	}

	@Override
	public void insertOrUpdateAssociation(AssociationKey key, Association association, AssociationContext associationContext) {
<<<<<<< HEAD
		log.info( "insertOrUpdateAssociation: AssociationKey:" + key + "; AssociationContext:" + associationContext + "; association:" + association );

		/*
		 * Tuple outEntityTuple = associationContext.getEntityTuple(); String inClassName = key.getTable(); String
		 * inBusinessPrimaryKeyName = EntityKeyUtil.findPrimaryKeyName( key.getEntityKey() ); Object
		 * inBusinessPrimaryKeyValue = EntityKeyUtil.findPrimaryKeyValue( key.getEntityKey() ); String edgeClassName =
		 * AssociationUtil.getMappedByFieldName( associationContext ); ORecordId outRid = (ORecordId)
		 * outEntityTuple.get( OrientDBConstant.SYSTEM_RID ); log.info( "insertOrUpdateAssociation: outRid:" + outRid +
		 * "; inClassName:" + inClassName + "; inBusinessPrimaryKeyName:" + inBusinessPrimaryKeyName +
		 * "; inBusinessPrimaryKeyValue:" + inBusinessPrimaryKeyValue + ";mappedBy:" + edgeClassName ); try { ORecordId
		 * inRid = EntityKeyUtil.findRid( provider.getConnection(), inClassName, inBusinessPrimaryKeyName,
		 * inBusinessPrimaryKeyValue ); if ( outRid == null ) { // try foun rid in db // @TODO search rid for 'out'
		 * direction throw new UnsupportedOperationException( "insertOrUpdateAssociation! Not supported yet." ); }
		 * AssociationUtil.removeAssociation( provider.getConnection(), edgeClassName, outRid, inRid );
		 * AssociationUtil.insertAssociation( provider.getConnection(), edgeClassName, outRid, inRid ); } catch
		 * (SQLException sqle) { log.error( "Error!", sqle ); throw new RuntimeException(
		 * "Can not insert or update association", sqle ); }
=======
		log.debug( "insertOrUpdateAssociation: AssociationKey:" + key + "; AssociationContext:" + associationContext + "; association:" + association );

		/*
		 * Tuple outEntityTuple = associationContext.getEntityTuple(); String inClassName = key.getTable(); String
		 * inBusinessPrimaryKeyName = EntityKeyUtil.findPrimaryKeyName(key.getEntityKey()); Object
		 * inBusinessPrimaryKeyValue = EntityKeyUtil.findPrimaryKeyValue(key.getEntityKey()); String edgeClassName =
		 * AssociationUtil.getMappedByFieldName(associationContext); ORecordId outRid = (ORecordId)
		 * outEntityTuple.get(OrientDBConstant.SYSTEM_RID); log.debug("insertOrUpdateAssociation: outRid:" + outRid +
		 * "; inClassName:" + inClassName + "; inBusinessPrimaryKeyName:" + inBusinessPrimaryKeyName +
		 * "; inBusinessPrimaryKeyValue:" + inBusinessPrimaryKeyValue + ";mappedBy:" + edgeClassName); try { ORecordId
		 * inRid = EntityKeyUtil.findRid(provider.getConnection(), inClassName, inBusinessPrimaryKeyName,
		 * inBusinessPrimaryKeyValue); if (outRid == null) { // try foun rid in db // @TODO search rid for 'out'
		 * direction throw new UnsupportedOperationException("insertOrUpdateAssociation! Not supported yet."); }
		 * AssociationUtil.removeAssociation(provider.getConnection(), edgeClassName, outRid, inRid);
		 * AssociationUtil.insertAssociation(provider.getConnection(), edgeClassName, outRid, inRid); } catch
		 * (SQLException sqle) { log.error("Error!", sqle); throw new RuntimeException(
		 * "Can not insert or update association", sqle); }
>>>>>>> 3712b2f73e6a708158478452211328c54279a26c
		 */

	}

	@Override
	public void removeAssociation(AssociationKey key, AssociationContext associationContext) {
<<<<<<< HEAD
		log.info( "removeAssociation: AssociationKey:" + key + "; AssociationContext:" + associationContext + ";" );
=======

		log.debug( "removeAssociation: AssociationKey:" + key + "; AssociationContext:" + associationContext + ";" );
>>>>>>> 3712b2f73e6a708158478452211328c54279a26c
		/*
		 * Tuple outEntityTuple = associationContext.getEntityTuple(); String inClassName = key.getTable(); Object
		 * inBusinessPrimaryKeyName = EntityKeyUtil.findPrimaryKeyName( key.getEntityKey() ); Object
		 * inBusinessPrimaryKeyValue = EntityKeyUtil.findPrimaryKeyValue( key.getEntityKey() ); String edgeClassName =
		 * AssociationUtil.getMappedByFieldName( associationContext ); ORecordId outRid = (ORecordId)
<<<<<<< HEAD
		 * outEntityTuple.get( OrientDBConstant.SYSTEM_RID ); log.info( "removeAssociation: outRid:" + outRid +
=======
		 * outEntityTuple.get( OrientDBConstant.SYSTEM_RID ); log.debug( "removeAssociation: outRid:" + outRid +
>>>>>>> 3712b2f73e6a708158478452211328c54279a26c
		 * "; inClassName:" + inClassName + "; inBusinessPrimaryKeyName:" + inBusinessPrimaryKeyName +
		 * "; inBusinessPrimaryKeyValue:" + inBusinessPrimaryKeyValue + ";mappedBy:" + edgeClassName ); try { ORecordId
		 * inRid = EntityKeyUtil.findRid( provider.getConnection(), inClassName, inClassName, inBusinessPrimaryKeyValue
		 * ); AssociationUtil.removeAssociation( provider.getConnection(), edgeClassName, outRid, inRid ); } catch
		 * (SQLException sqle) { log.error( "Error!", sqle ); throw new RuntimeException(
		 * "Can not insert or update association", sqle ); }
		 */
	}

	@Override
	public boolean isStoredInEntityStructure(AssociationKeyMetadata associationKeyMetadata, AssociationTypeContext associationTypeContext) {
		return true;
	}

	@Override
	public Number nextValue(NextValueRequest request) {
<<<<<<< HEAD
		log.info( "NextValueRequest:" + request + "; " );
		// return ORecordId.EMPTY_RECORD_ID.getClusterPosition();
		throw new UnsupportedOperationException( "nextValue Not supported yet." );
=======
		log.debug( "NextValueRequest:" + request + "; " );
		Number nextValue = null;
		IdSourceType type = request.getKey().getMetadata().getType();
		if ( IdSourceType.SEQUENCE.equals( type ) ) {
			String seqName = request.getKey().getMetadata().getName();
			nextValue = SequenceUtil.getSequence( provider.getConnection(), seqName );
		}
		else {
			throw new UnsupportedOperationException( "nextValue Not supported yet." );
		}
		return nextValue;
>>>>>>> 3712b2f73e6a708158478452211328c54279a26c
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
<<<<<<< HEAD
			log.info( "executeBackendQuery.native query: " + nativeQuery );
			PreparedStatement pstmt = provider.getConnection().prepareStatement( nativeQuery );
			for ( Map.Entry<String, TypedGridValue> entry : queryParameters.getNamedParameters().entrySet() ) {
				String key = entry.getKey();
				TypedGridValue value = entry.getValue();
				log.info( "key: " + key + "; type:" + value.getType().getName() + "; value:" + value.getValue() );
				// @todo move to Map
				if ( value.getType().getName().equals( "string" ) ) {
					pstmt.setString( 1, (String) value.getValue() );
				}
				if ( value.getType().getName().equals( "long" ) ) {
					pstmt.setLong( 1, (Long) value.getValue() );
				}

			}
			ResultSet rs = pstmt.executeQuery();

			/*
			 * if (backendQuery.getSingleEntityKeyMetadataOrNull() != null) { return new NodesTupleIterator(result,
			 * backendQuery.getSingleEntityKeyMetadataOrNull()); }
			 */
			return new ResultSetTupleIterator( rs );
		}
		catch (SQLException e) {
			log.error( "Error with ResultSet", e );
			throw new RuntimeException( e );
=======
			PreparedStatement pstmt = provider.getConnection().prepareStatement( nativeQuery );
			int paramIndex = 1;
			for ( Map.Entry<String, TypedGridValue> entry : queryParameters.getNamedParameters().entrySet() ) {
				String key = entry.getKey();
				TypedGridValue value = entry.getValue();
				log.debug( "key: " + key + "; type:" + value.getType().getName() + "; value:" + value.getValue() );
				try {
					// @todo move to Map
					if ( value.getType().getName().equals( "string" ) ) {
						pstmt.setString( 1, (String) value.getValue() );
					}
					else if ( value.getType().getName().equals( "long" ) ) {
						pstmt.setLong( 1, (Long) value.getValue() );
					}
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
>>>>>>> 3712b2f73e6a708158478452211328c54279a26c
		}
	}

	private String buildNativeQuery(BackendQuery<String> customQuery, QueryParameters queryParameters) {
<<<<<<< HEAD
		StringBuilder nativeQuery = new StringBuilder( customQuery.getQuery() );
		log.info( "2.buildNativeQuery.native query: " + customQuery.getQuery() );
		return nativeQuery.toString();
=======
		return customQuery.getQuery();
>>>>>>> 3712b2f73e6a708158478452211328c54279a26c
	}

	/**
	 * Returns a map with the named parameter values from the given parameters object, converted by the {@link GridType}
	 * corresponding to each parameter type.
	 */
	private Map<String, Object> getNamedParameterValuesConvertedByGridType(QueryParameters queryParameters) {
<<<<<<< HEAD
		log.info( "getNamedParameterValuesConvertedByGridType. named parameters: " + queryParameters.getNamedParameters().size() );
=======
		log.debug( "getNamedParameterValuesConvertedByGridType. named parameters: " + queryParameters.getNamedParameters().size() );
>>>>>>> 3712b2f73e6a708158478452211328c54279a26c
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
<<<<<<< HEAD
		log.info( "1.parseNativeQuery.native query: " + nativeQuery );
		// We return given native SQL query as they is; Currently there is no API for validating OrientDB queries
		// without
		// actually executing them
=======
>>>>>>> 3712b2f73e6a708158478452211328c54279a26c
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
<<<<<<< HEAD
		log.info( "entityQueries:" + entityQueries );
		log.info( "sessionFactoryCreated" );
=======

		log.debug( "entityQueries:" + entityQueries );
		log.debug( "sessionFactoryCreated" );
>>>>>>> 3712b2f73e6a708158478452211328c54279a26c
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
<<<<<<< HEAD
				log.info( "initializeAssociationQueries: ogmCollectionPersister :" + ogmCollectionPersister );
				EntityKeyMetadata ownerEntityKeyMetadata = ( (OgmEntityPersister) ( ogmCollectionPersister.getOwnerEntityPersister() ) ).getEntityKeyMetadata();
				log.info( "initializeAssociationQueries: ownerEntityKeyMetadata :" + ownerEntityKeyMetadata );
				AssociationKeyMetadata associationKeyMetadata = ogmCollectionPersister.getAssociationKeyMetadata();
				log.info( "initializeAssociationQueries: associationKeyMetadata :" + associationKeyMetadata );
=======

				log.debug( "initializeAssociationQueries: ogmCollectionPersister :" + ogmCollectionPersister );
				EntityKeyMetadata ownerEntityKeyMetadata = ( (OgmEntityPersister) ( ogmCollectionPersister.getOwnerEntityPersister() ) ).getEntityKeyMetadata();

				log.debug( "initializeAssociationQueries: ownerEntityKeyMetadata :" + ownerEntityKeyMetadata );
				AssociationKeyMetadata associationKeyMetadata = ogmCollectionPersister.getAssociationKeyMetadata();

				log.debug( "initializeAssociationQueries: associationKeyMetadata :" + associationKeyMetadata );
>>>>>>> 3712b2f73e6a708158478452211328c54279a26c
				queryMap.put( associationKeyMetadata, new OrientDBAssociationQueries( ownerEntityKeyMetadata, associationKeyMetadata ) );
			}
		}
		return queryMap;
	}

	@Override
	public GridType overrideType(Type type) {
<<<<<<< HEAD
		// log.info( "overrideType:" + type.getName() + ";" + type.getReturnedClass() );
=======
		log.debug( "overrideType:" + type.getName() + ";" + type.getReturnedClass() + ";" );
>>>>>>> 3712b2f73e6a708158478452211328c54279a26c
		GridType gridType = null;

		if ( type.getName().equals( ORecordId.class.getName() ) ) {
			gridType = ORecordIdGridType.INSTANCE;
		}
		else if ( type.getName().equals( ORidBag.class.getName() ) ) {
			gridType = ORidBagGridType.INSTANCE;
		}
		else if ( type.getName().equals( ORidBag.class.getName() ) ) {
			gridType = ORidBagGridType.INSTANCE;
		} // persist calendars as ISO8601 strings, including TZ info
		/*
		 * else if ( type == StandardBasicTypes.CALENDAR ) { gridType = Iso8601CalendarGridType.DATETIME_INSTANCE; }
		 * else if ( type == StandardBasicTypes.CALENDAR_DATE ) { gridType = Iso8601CalendarGridType.DATE_INSTANCE; }
		 * else if ( type == StandardBasicTypes.DATE ) { return Iso8601DateGridType.DATE_INSTANCE; } else if ( type ==
		 * StandardBasicTypes.TIME ) { return Iso8601DateGridType.DATETIME_INSTANCE; } else if ( type ==
		 * StandardBasicTypes.TIMESTAMP ) { return Iso8601DateGridType.DATETIME_INSTANCE; }
		 */

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

}
