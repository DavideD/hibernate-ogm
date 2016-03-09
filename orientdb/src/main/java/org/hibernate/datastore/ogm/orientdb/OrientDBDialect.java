/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.datastore.ogm.orientdb;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.tinkerpop.blueprints.impls.orient.OrientDynaElementIterable;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import com.tinkerpop.blueprints.impls.orient.OrientVertexType;
import org.hibernate.datastore.ogm.orientdb.dialect.impl.OrientDBQueryHelper;
import org.hibernate.datastore.ogm.orientdb.dialect.impl.OrientDBTupleSnapshot;
import org.hibernate.datastore.ogm.orientdb.dialect.impl.OrientTransientVertex;
import org.hibernate.datastore.ogm.orientdb.impl.OrientDBDatastoreProvider;
import org.hibernate.datastore.ogm.orientdb.logging.impl.Log;
import org.hibernate.datastore.ogm.orientdb.logging.impl.LoggerFactory;
import org.hibernate.datastore.ogm.orientdb.type.spi.ORecordIdGridType;
import org.hibernate.ogm.dialect.spi.*;
import org.hibernate.ogm.model.key.spi.AssociationKey;
import org.hibernate.ogm.model.key.spi.AssociationKeyMetadata;
import org.hibernate.ogm.model.key.spi.EntityKey;
import org.hibernate.ogm.model.key.spi.EntityKeyMetadata;
import org.hibernate.ogm.model.spi.Association;
import org.hibernate.ogm.model.spi.Tuple;
import org.hibernate.ogm.model.spi.TupleOperation;
import org.hibernate.ogm.model.spi.TupleOperationType;
import org.hibernate.ogm.type.spi.GridType;
import org.hibernate.type.Type;

import java.util.Set;

/**
 * @author Sergey Chernolyas (sergey.chernolyas@gmail.com)
 * @author cristhiank (calovi86 at gmail.com)
 */
public class OrientDBDialect extends BaseGridDialect {

	private static final Log log = LoggerFactory.getLogger();
	private OrientDBDatastoreProvider provider;

	public OrientDBDialect(OrientDBDatastoreProvider provider) {
		this.provider = provider;
	}

	public OrientVertex findRecordByKey(EntityKey key, TupleContext tupleContext) {
		OrientGraph connection = provider.getConnection();
		//TODO: include tupleContext selectableColumns projection ??
		OSQLSynchQuery<ODocument> orientQuery = OrientDBQueryHelper.createSelect( key );
		checkIfClassExists( key.getTable(), connection );
		OrientDynaElementIterable result = connection.command( orientQuery ).execute();
		if ( result.iterator().hasNext() ) {
			return (OrientVertex) result.iterator().next();
		}
		return null;
	}

	private OrientVertex prepareTransientObjectWithPk(EntityKey key) {
		OrientVertex object = new OrientTransientVertex();
		String[] columnNames = key.getColumnNames();
		Object[] columnValues = key.getColumnValues();
		for ( int i = 0; i < columnNames.length; i++ ) {
			object.setProperty( columnNames[i], columnValues[i] );
		}
		return object;
	}

	@Override
	public Tuple getTuple(EntityKey key, TupleContext tupleContext) {
		OrientVertex result = findRecordByKey( key, tupleContext );
		OrientDBTupleSnapshot tuple;
		if ( result != null ) {
			tuple = new OrientDBTupleSnapshot( result, key.getMetadata() );
			return new Tuple( tuple );
		}
		return null;
	}

	@Override
	public Tuple createTuple(EntityKey key, TupleContext tupleContext) {
		log.info( "createTuple:EntityKey:" + key + "; tupleContext" + tupleContext );
		OrientVertex newObject = prepareTransientObjectWithPk( key );
		return new Tuple( new OrientDBTupleSnapshot( newObject, key.getMetadata() ) );
	}

	@Override
	public void insertOrUpdateTuple(EntityKey key, Tuple tuple, TupleContext tupleContext) throws TupleAlreadyExistsException {
		OrientGraph connection = provider.getConnection();
		OrientVertex recordByKey = findRecordByKey( key, tupleContext );
		if ( recordByKey == null ) {
			//Doesn't exist, create it.
			createOrientDBRecord( key, tuple, tupleContext, connection );
		}
		else {
			updateOrientDBRecord( recordByKey, key, tuple, tupleContext, connection );
		}
	}

	private void updateOrientDBRecord(OrientVertex originalRecord, EntityKey key, Tuple tuple, TupleContext tupleContext, OrientGraph connection) {
		processTupleOperations( tuple.getOperations(), originalRecord );
		originalRecord.save();
	}

	private void createOrientDBRecord(EntityKey key, Tuple tuple, TupleContext tupleContext, OrientGraph connection) {
		//TODO: support adding records in clusters.
		checkIfClassExists( key.getTable(), connection );
		OrientVertex record = connection.addVertex( "class:" + key.getTable() );
		processTupleOperations( tuple.getOperations(), record );
	}

	private void processTupleOperations(Set<TupleOperation> operations, OrientVertex record) {
		for ( TupleOperation op : operations ) {
			String column = op.getColumn();
			TupleOperationType type = op.getType();
			switch ( type ) {
				case PUT:
					record.setProperty( column, op.getValue() );
					break;
				case REMOVE:
					record.removeProperty( column );
					break;
				case PUT_NULL:
					record.setProperty( column, null );
			}
		}
	}

	private void checkIfClassExists(String tableName, OrientGraph connection) {
		OrientVertexType vertexType = connection.getVertexType( tableName );
		if ( vertexType == null ) {
			//The vertex class doesn't exist, inconsistent database schema.
			throw log.classDoesntExists( tableName );
		}
	}

	@Override
	public void removeTuple(EntityKey key, TupleContext tupleContext) {
		OrientGraph connection = provider.getConnection();
		OrientVertex recordByKey = findRecordByKey( key, tupleContext );
		if ( recordByKey != null ) {
			connection.removeVertex( recordByKey );
		}
		//TODO: validate if it doesn't exist.
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
		return ORecordId.EMPTY_RECORD_ID.getClusterPosition();
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
	public GridType overrideType(Type type) {
		log.info( "overrideType:" + type.getName() + ";" + type.getReturnedClass() );
		GridType gridType;
		if ( type.getName().equals( "com.orientechnologies.orient.core.id.ORecordId" ) ) {
			gridType = ORecordIdGridType.INSTANCE;
		}
		else {
			gridType = super.overrideType( type ); // To change body of generated methods, choose Tools | Templates.
		}
		return gridType;
	}
}
