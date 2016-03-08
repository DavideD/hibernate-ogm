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
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import org.hibernate.datastore.ogm.orientdb.dialect.impl.OrientDBQueryHelper;
import org.hibernate.datastore.ogm.orientdb.dialect.impl.OrientDBTupleSnapshot;
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

	public OrientVertex findRecordByKey(OrientGraph executionEngine, EntityKey key, TupleContext tupleContext) {

		//TODO: include tupleContext selectableColumns projection ??
		OSQLSynchQuery<ODocument> orientQuery = OrientDBQueryHelper.createSelect( key );
		return executionEngine.command( orientQuery ).execute();
	}

	private OrientVertex prepareObjectWithPk(EntityKey key) {
		OrientVertex object = new OrientVertex();
		String[] columnNames = key.getColumnNames();
		Object[] columnValues = key.getColumnValues();
		for ( int i = 0; i < columnNames.length; i++ ) {
			object.setProperty( columnNames[i], columnValues[i] );
		}
		return object;
	}

	@Override
	public Tuple getTuple(EntityKey key, TupleContext tupleContext) {
		OrientVertex document = findRecordByKey( provider.getConnection(), key, tupleContext );
		OrientDBTupleSnapshot tuple;
		if ( document == null ) {
			tuple = new OrientDBTupleSnapshot( prepareObjectWithPk( key ), key.getMetadata(), OrientDBTupleSnapshot.SnapshotType.INSERT );
		}
		else {
			return null;
		}
		return new Tuple( tuple );
	}

	@Override
	public Tuple createTuple(EntityKey key, TupleContext tupleContext) {
		log.info( "createTuple:EntityKey:" + key + "; tupleContext" + tupleContext );
		OrientVertex newObject = prepareObjectWithPk( key );
		return new Tuple( new OrientDBTupleSnapshot( newObject, key.getMetadata(), OrientDBTupleSnapshot.SnapshotType.INSERT ) );
	}

	@Override
	public void insertOrUpdateTuple(EntityKey key, Tuple tuple, TupleContext tupleContext) throws TupleAlreadyExistsException {
		log.info( "insertOrUpdateTuple:EntityKey:" + key + "; tupleContext" + tupleContext + "; tuple:" + tuple );
		OrientGraph connection = provider.getConnection();
		OrientVertex recordByKey = findRecordByKey( connection, key, tupleContext );
		if ( recordByKey == null ) {
			//Doesn't exist, create it.
			createOrientDBRecord( key, tuple, tupleContext, connection );
		}
	}

	private void createOrientDBRecord(EntityKey key, Tuple tuple, TupleContext tupleContext, OrientGraph connection) {
		Set<TupleOperation> operations = tuple.getOperations();
		for ( TupleOperation op : operations ) {
			String column = op.getColumn();

		}
	}

	@Override
	public void removeTuple(EntityKey key, TupleContext tupleContext) {
		log.info( "removeTuple:EntityKey:" + key + "; tupleContext" + tupleContext );
		throw new UnsupportedOperationException( "Not supported yet." );
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
