/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.datastore.ogm.orientdb.dialect.impl;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.hibernate.datastore.ogm.orientdb.constant.OrientDBConstant;
import org.hibernate.datastore.ogm.orientdb.logging.impl.Log;
import org.hibernate.datastore.ogm.orientdb.logging.impl.LoggerFactory;
import org.hibernate.datastore.ogm.orientdb.utils.EntityKeyUtil;
import org.hibernate.ogm.dialect.spi.AssociationContext;
import org.hibernate.ogm.model.key.spi.AssociationKey;
import org.hibernate.ogm.model.key.spi.EntityKey;
import org.hibernate.ogm.model.key.spi.EntityKeyMetadata;
import org.hibernate.ogm.model.spi.Tuple;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.Arrays;
import org.hibernate.datastore.ogm.orientdb.utils.ODocumentUtil;

/**
 * @author Sergey Chernolyas (sergey.chernolyas@gmail.com)
 */
public class OrientDBEntityQueries extends QueriesBase {

	private static Log log = LoggerFactory.getLogger();

	private final EntityKeyMetadata entityKeyMetadata;

	public OrientDBEntityQueries(EntityKeyMetadata entityKeyMetadata) {
		this.entityKeyMetadata = entityKeyMetadata;
		for ( int i = 0; i < entityKeyMetadata.getColumnNames().length; i++ ) {
			String columnName = entityKeyMetadata.getColumnNames()[i];
			log.debugf( "column number: %d ; column name: %s", i, columnName );
		}
	}

	/**
	 * Find the node corresponding to an entity.
	 *
	 * @param connection the connection
	 * @param entityKey
	 * @return the corresponding node
	 */

	public Map<String, Object> findEntity(Connection connection, EntityKey entityKey) {
		Map<String, Object> dbValues = new LinkedHashMap<>();
		StringBuilder query = new StringBuilder( "select from " );
		try {
			Statement stmt = connection.createStatement();
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
			ResultSet rs = stmt.executeQuery( query.toString() );
			if ( rs.next() ) {
				ResultSetMetaData metadata = rs.getMetaData();
				for ( String systemField : OrientDBConstant.SYSTEM_FIELDS ) {
					dbValues.put( systemField, rs.getObject( systemField ) );
				}
				for ( int i = 0; i < rs.getMetaData().getColumnCount(); i++ ) {
					int dbFieldNo = i + 1;
					String dbColumnName = metadata.getColumnName( dbFieldNo );
					/*
					 * if ( isLinkedProperty( dbColumnName ) ) { continue; }
					 */
					Object dbValue = rs.getObject( dbColumnName );
					log.debugf( "%d dbColumnName: %s dbValue class:", i, dbColumnName, ( dbValue != null ? dbValue.getClass() : null ) );
					log.debugf( "%d dbColumnName: %s ; sql type: %s", i, dbColumnName, rs.getMetaData().getColumnTypeName( dbFieldNo ) );
					dbValues.put( dbColumnName, dbValue );
					if ( dbValue instanceof Date ) {
						String format = rs.getMetaData().getColumnTypeName( dbFieldNo ).equals( "DATETIME" )
								? OrientDBConstant.DATETIME_FORMAT
								: OrientDBConstant.DATE_FORMAT;
						dbValues.put( dbColumnName, new SimpleDateFormat( format ).format( dbValue ) );
					}
					else if ( dbValue instanceof ODocument ) {
						dbValues.remove( dbColumnName );
						dbValues.putAll( ODocumentUtil.extractNamesTree( dbColumnName, (ODocument) dbValue ) );
					}
				}
				reCastValues( dbValues );
				log.debugf( " entity values from db:  %s", dbValues );
			}
			else {
				log.debugf( " entity by primary key %s not found!", entityKey );
				return null;
			}
			return dbValues;
		}
		catch (SQLException sqle) {
			throw log.cannotExecuteQuery( query.toString(), sqle );

		}
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

	private boolean isLinkedProperty(String propertyName) {
		for ( String linkFieldStarts : OrientDBConstant.LINK_FIELDS ) {
			if ( propertyName.startsWith( linkFieldStarts ) ) {
				return true;
			}
		}
		return false;
	}

	public List<Map<String, Object>> findAssociation(Connection connection, AssociationKey associationKey,
			AssociationContext associationContext) {
		List<Map<String, Object>> association = new LinkedList<>();
		log.debugf( "findAssociation: associationKey: %s; associationContext: %s", associationKey, associationContext );
		StringBuilder query = new StringBuilder( 100 );
		query.append( "SELECT FROM " ).append( associationKey.getTable() ).append( " WHERE " );
		for ( int i = 0; i < associationKey.getColumnNames().length; i++ ) {
			String name = associationKey.getColumnNames()[i];
			Object value = associationKey.getColumnValues()[i];
			query.append( name ).append( "=" );
			EntityKeyUtil.setFieldValue( query, value );
		}
		log.debugf( "findAssociation: query: %s", query );
		try {
			Statement stmt = connection.createStatement();
			ResultSet rs = stmt.executeQuery( query.toString() );
			while ( rs.next() ) {
				Map<String, Object> dbValues = new LinkedHashMap<>();
				ResultSetMetaData metadata = rs.getMetaData();
				for ( String systemField : OrientDBConstant.SYSTEM_FIELDS ) {
					dbValues.put( systemField, rs.getObject( systemField ) );
				}
				for ( int i = 0; i < rs.getMetaData().getColumnCount(); i++ ) {
					int dbFieldNo = i + 1;
					String dbColumnName = metadata.getColumnName( dbFieldNo );
					if ( isLinkedProperty( dbColumnName ) ) {
						continue;
					}
					Object dbValue = rs.getObject( dbColumnName );
					dbValues.put( dbColumnName, dbValue );
				}
				log.debugf( " entiry values from db: %s", dbValues );
				association.add( dbValues );
			}
			log.debugf( "findAssociation: rows %d:" + association.size() );
		}
		catch (SQLException sqle) {
			throw log.cannotExecuteQuery( query.toString(), sqle );

		}
		return association;
	}

	private ORecordId extractRid(AssociationContext associationContext) {
		Tuple tuple = associationContext.getEntityTuple();
		return (ORecordId) tuple.get( OrientDBConstant.SYSTEM_RID );
	}

	private String getRelationshipType(AssociationContext associationContext) {
		return associationContext.getAssociationTypeContext().getRoleOnMainSide();
	}

	/**
	 * no links of the type (class) in DB. this is not error
	 *
	 * @param sqle
	 * @return
	 */
	private boolean isClassNotFoundInDB(SQLException sqle) {
		boolean result = false;
		for ( Iterator<Throwable> iterator = sqle.iterator(); ( iterator.hasNext() || result ); ) {
			Throwable t = iterator.next();
			// log.debug( "findAssociation: Throwable message :"+t.getMessage());
			result = t.getMessage().contains( "was not found in database" );
		}

		return result;
	}

}
