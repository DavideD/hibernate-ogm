/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.orientdb.dialect.impl;

import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Arrays;

import org.hibernate.ogm.datastore.orientdb.logging.impl.Log;
import org.hibernate.ogm.datastore.orientdb.logging.impl.LoggerFactory;
import org.hibernate.ogm.datastore.orientdb.utils.EntityKeyUtil;
import org.hibernate.ogm.dialect.spi.AssociationContext;
import org.hibernate.ogm.model.key.spi.AssociationKey;
import org.hibernate.ogm.model.key.spi.EntityKey;
import org.hibernate.ogm.model.key.spi.EntityKeyMetadata;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.hibernate.ogm.datastore.orientdb.utils.ODocumentUtil;
import org.hibernate.ogm.datastore.orientdb.utils.NativeQueryUtil;

/**
 * Container for the queries related to one entity type in OrientDB.
 *
 * @author Sergey Chernolyas &lt;sergey.chernolyas@gmail.com&gt;
 */
public class OrientDBEntityQueries extends QueriesBase {

	private static Log log = LoggerFactory.getLogger();

	private final EntityKeyMetadata entityKeyMetadata;

	/**
	 * Contractor
	 *
	 * @param entityKeyMetadata metadata of entity keys
	 */
	public OrientDBEntityQueries(EntityKeyMetadata entityKeyMetadata) {
		this.entityKeyMetadata = entityKeyMetadata;
		for ( int i = 0; i < entityKeyMetadata.getColumnNames().length; i++ ) {
			String columnName = entityKeyMetadata.getColumnNames()[i];
			log.debugf( "column number: %d ; column name: %s", i, columnName );
		}
	}

	/**
	 * Find the node corresponding to the entity key.
	 *
	 * @param db database instance
	 * @param entityKey entity key
	 * @return the corresponding node
	 */

	public ODocument findEntity(ODatabaseDocumentTx db, EntityKey entityKey) {
		Map<String, Object> dbValues = new LinkedHashMap<>();
		StringBuilder query = new StringBuilder( "select from " );

		if ( entityKey.getColumnNames().length == 1 && entityKey.getColumnValues()[0] instanceof ORecordId ) {
			// search by @rid
			ORecordId rid = (ORecordId) entityKey.getColumnValues()[0];
			query.append( rid );
		}
		else {
			// search by business key
			log.debugf( "column names: %s", Arrays.asList( entityKey.getColumnNames() ) );
			query.append( entityKey.getTable() ).append( " WHERE " ).append( EntityKeyUtil.generatePrimaryKeyPredicate( entityKey ) );

		}
		log.debugf( "find entiry query: %s", query.toString() );
		List<ODocument> documents = NativeQueryUtil.executeIdempotentQuery( db, query );
		if ( documents.isEmpty() ) {
			log.debugf( " entity by primary key %s not found!", entityKey );
			return null;
		}
		/*ODocument currentDocument = documents.get( 0 );
		dbValues = currentDocument.toMap();
		for ( String fieldName : dbValues.keySet() ) {
			Object fieldValue = dbValues.get( fieldName );
			if ( fieldValue instanceof ODocument ) {
				dbValues.remove( fieldName );
				dbValues.putAll( ODocumentUtil.extractNamesTree( fieldName, (ODocument) fieldValue ) );
			}

		}
		reCastValues( dbValues );		
		return dbValues; */
                return documents.get( 0 );
	}

	private void reCastValues(Map<String, Object> map) {
		for ( Map.Entry<String, Object> entry : map.entrySet() ) {
			String key = entry.getKey();
			Object value = entry.getValue();
			if ( value instanceof BigDecimal ) {
				BigDecimal bd = (BigDecimal) value;
				entry.setValue( bd.toString() );
			}

		}
	}

	/**
	 * find association that corresponding to the association key.
	 *
	 * @param connection connection to OrientDB
	 * @param associationKey association key
	 * @param associationContext context
	 * @return list of associations
	 */

	public List<Map<String, Object>> findAssociation(ODatabaseDocumentTx db, AssociationKey associationKey,
			AssociationContext associationContext) {
		List<Map<String, Object>> association = new LinkedList<>();
		log.debugf( "findAssociation: associationKey: %s; associationContext: %s", associationKey, associationContext );
		log.debugf( "findAssociation: associationKeyMetadata: %s", associationKey.getMetadata() );

		StringBuilder query = new StringBuilder( 100 );
		query.append( "SELECT FROM " ).append( associationKey.getTable() ).append( " WHERE " );
		for ( int i = 0; i < associationKey.getColumnNames().length; i++ ) {
			String name = associationKey.getColumnNames()[i];
			Object value = associationKey.getColumnValues()[i];
			query.append( name ).append( "=" );
			EntityKeyUtil.setFieldValue( query, value );
		}

		String[] indexColumns = associationKey.getMetadata().getRowKeyIndexColumnNames();
		if ( indexColumns != null && indexColumns.length > 0 ) {
			query.append( " order by " );
			for ( String indexColumn : indexColumns ) {
				query.append( indexColumn ).append( " asc " ).append( "," );
			}
			query.setLength( query.length() - 1 );
		}

		log.debugf( "findAssociation: query: %s", query );
		List<ODocument> documents = NativeQueryUtil.executeIdempotentQuery( db, query );
		for ( ODocument doc : documents ) {
			Map<String, Object> dbValues = ODocumentUtil.toMap( doc );
			for ( String fieldName : dbValues.keySet() ) {
				if ( dbValues.get( fieldName ) instanceof ODocument ) {
					dbValues.put( fieldName, ODocumentUtil.extractNamesTree( fieldName, (ODocument) dbValues.get( fieldName ) ) );
				}
			}
			association.add( dbValues );
		}
		log.debugf( "findAssociation: rows :  %d", association.size() );

		/*
		 * try { Statement stmt = connection.createStatement(); ResultSet rs = stmt.executeQuery( query.toString() );
		 * while ( rs.next() ) { Map<String, Object> dbValues = new LinkedHashMap<>(); ResultSetMetaData metadata =
		 * rs.getMetaData(); for ( String systemField : OrientDBConstant.SYSTEM_FIELDS ) { dbValues.put( systemField,
		 * rs.getObject( systemField ) ); } for ( int i = 0; i < rs.getMetaData().getColumnCount(); i++ ) { int
		 * dbFieldNo = i + 1; String dbColumnName = metadata.getColumnName( dbFieldNo ); if ( isLinkedProperty(
		 * dbColumnName ) ) { continue; } Object dbValue = rs.getObject( dbColumnName ); dbValues.put( dbColumnName,
		 * dbValue ); if ( dbValue instanceof ODocument ) { dbValues.remove( dbColumnName ); dbValues.putAll(
		 * ODocumentUtil.extractNamesTree( dbColumnName, (ODocument) dbValue ) ); } } log.debugf(
		 * " entiry values from db: %s", dbValues ); association.add( dbValues ); } log.debugf(
		 * "findAssociation: rows :  %d", association.size() ); } catch (SQLException sqle) { throw
		 * log.cannotExecuteQuery( query.toString(), sqle ); }
		 */
		return association;
	}
}
