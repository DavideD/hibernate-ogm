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
import org.hibernate.ogm.model.impl.DefaultEntityKeyMetadata;
import org.hibernate.ogm.model.key.spi.AssociationKey;
import org.hibernate.ogm.model.key.spi.EntityKey;
import org.hibernate.ogm.model.key.spi.EntityKeyMetadata;
import org.hibernate.ogm.model.spi.Tuple;

import com.orientechnologies.orient.core.id.ORecordId;

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
			log.debug( "column number:" + i + "; column name:" + columnName );
		}

	}

	/**
	 * Find the node corresponding to an entity.
	 *
	 * @param executionEngine the {@link GraphDatabaseService} used to run the query
	 * @param columnValues the values in {@link org.hibernate.ogm.model.key.spi.EntityKey#getColumnValues()}
	 * @return the corresponding node
	 * @throws java.sql.SQLException
	 */

	public Map<String, Object> findEntity(Connection executionEngine, EntityKey entityKey) throws SQLException {
		Map<String, Object> params = params( entityKey.getColumnValues() );
		Map<String, Object> dbValues = new LinkedHashMap<>();
		Object dbKeyValue = EntityKeyUtil.findPrimaryKeyValue( entityKey );
		String dbKeyName = EntityKeyUtil.findPrimaryKeyName( entityKey );

		Statement stmt = executionEngine.createStatement();
		StringBuilder query = new StringBuilder( "select from " );
		if ( params.size() == 1 ) {
			if ( dbKeyValue instanceof ORecordId ) {
				// search by @rid
				ORecordId rid = (ORecordId) dbKeyValue;
				query.append( rid );
			}
			else {
				// search by business key
				query.append( entityKey.getTable() ).append( " WHERE " ).append( dbKeyName ).append( " = " );
				EntityKeyUtil.setFieldValue( query, dbKeyValue );
			}
		}
		else {
			// query.append(entityKeyMetadata.getTable());
			throw new UnsupportedOperationException( "Not supported yet." );
		}

		log.debug( "find entiry query: " + query.toString() );

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
				log.debug( i + " dbColumnName " + dbColumnName + "; dbValue class:" + ( dbValue != null ? dbValue.getClass() : null ) );
				log.debug( i + " dbColumnName " + dbColumnName + "; sql type:" + rs.getMetaData().getColumnTypeName( dbFieldNo ) );
				dbValues.put( dbColumnName, dbValue );
				if ( dbValue != null && dbValue.getClass().equals( Date.class ) ) {
					String format = rs.getMetaData().getColumnTypeName( dbFieldNo ).equals( "DATETIME" )
							? OrientDBConstant.DATETIME_FORMAT
									: OrientDBConstant.DATE_FORMAT;
					dbValues.put( dbColumnName, new SimpleDateFormat( format ).format( dbValue ) );
				}
			}
			reCastValues( dbValues );
			log.debug( " entiry values from db: " + dbValues );
		}
		else {
			return null;
		}

		return dbValues;
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

	private EntityKey createEntityKeyByRid(ORecordId rid) {
		return new EntityKey( new DefaultEntityKeyMetadata( "", new String[]{ OrientDBConstant.SYSTEM_RID } ), new Object[]{ rid } );
	}

	public List<Map<String, Object>> findAssociation(Connection connection, AssociationKey associationKey, AssociationContext associationContext)
			throws SQLException {
		List<Map<String, Object>> association = new LinkedList<>();
		log.debug( "findAssociation: associationKey:" + associationKey + "; associationContext:" + associationContext );

		StringBuilder query = new StringBuilder( 100 );
		query.append( "SELECT FROM " ).append( associationKey.getTable() ).append( " WHERE " );
		for ( int i = 0; i < associationKey.getColumnNames().length; i++ ) {
			String name = associationKey.getColumnNames()[i];
			Object value = associationKey.getColumnValues()[i];
			query.append( name ).append( "=" );
			EntityKeyUtil.setFieldValue( query, value );
		}

		log.debug( "findAssociation: query:" + query );

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
			log.debug( " entiry values from db: " + dbValues );
			association.add( dbValues );
		}
		log.debug( "findAssociation: edges:" + association.size() );

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

	private String findColumnByName(String name) {
		String index = "-1";
		for ( int i = 0; i < entityKeyMetadata.getColumnNames().length; i++ ) {
			String columnName = entityKeyMetadata.getColumnNames()[i];
			if ( columnName.equals( name ) ) {
				index = String.valueOf( i );
				break;
			}

		}
		return index;
	}

	private String findColumnByNum(int num) {
		return !( num > entityKeyMetadata.getColumnNames().length - 1 ) ? entityKeyMetadata.getColumnNames()[num] : null;
	}

}
