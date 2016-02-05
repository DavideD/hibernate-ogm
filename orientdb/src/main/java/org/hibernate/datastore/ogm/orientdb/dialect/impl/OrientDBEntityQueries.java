/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.datastore.ogm.orientdb.dialect.impl;

import com.orientechnologies.orient.core.id.ORecordId;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.Map;
import org.hibernate.datastore.ogm.orientdb.constant.OrientDBConstant;
import org.hibernate.datastore.ogm.orientdb.logging.impl.Log;
import org.hibernate.datastore.ogm.orientdb.logging.impl.LoggerFactory;
import org.hibernate.datastore.ogm.orientdb.utils.EntityKeyUtil;
import org.hibernate.ogm.model.key.spi.EntityKey;
import org.hibernate.ogm.model.key.spi.EntityKeyMetadata;

/**
 * @author Sergey Chernolyas (sergey.chernolyas@gmail.com)
 */
public class OrientDBEntityQueries extends QueriesBase {

	private static Log LOG = LoggerFactory.getLogger();

	private final EntityKeyMetadata entityKeyMetadata;

	public OrientDBEntityQueries(EntityKeyMetadata entityKeyMetadata) {
		this.entityKeyMetadata = entityKeyMetadata;
		for ( int i = 0; i < entityKeyMetadata.getColumnNames().length; i++ ) {
			String columnName = entityKeyMetadata.getColumnNames()[i];
			LOG.info( "column number:" + i + "; column name:" + columnName );
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
			query.append( entityKey.getTable() ).append( " WHERE " ).append( dbKeyName ).append( " = " );
			EntityKeyUtil.setPrimaryKeyValue( query, dbKeyValue );
		}
		else {
			// query.append(entityKeyMetadata.getTable());
			throw new UnsupportedOperationException( "Not supported yet." );
		}

		LOG.info( "find entiry query: " + query.toString() );

		ResultSet rs = stmt.executeQuery( query.toString() );
		if ( rs.next() ) {
			ResultSetMetaData metadata = rs.getMetaData();
			for ( String systemField : OrientDBConstant.SYSTEM_FIELDS ) {
				dbValues.put( systemField, rs.getObject( systemField ) );

			}
			for ( int i = 0; i < rs.getMetaData().getColumnCount(); i++ ) {
				int dbFieldNo = i + 1;
				String dbColumnName = metadata.getColumnName( dbFieldNo );
				LOG.info( i + " dbColumnName " + dbColumnName );
				dbValues.put( dbColumnName, rs.getObject( dbColumnName ) );
			}
			LOG.info( " entiry values from db: " + dbValues );
		}
		else {
			return null;
		}
		return dbValues;
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
