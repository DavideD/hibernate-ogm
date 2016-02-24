/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */

package org.hibernate.datastore.ogm.orientdb.utils;

import com.orientechnologies.orient.core.id.ORecordId;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.UUID;
import org.hibernate.datastore.ogm.orientdb.constant.OrientDBConstant;
import org.hibernate.datastore.ogm.orientdb.logging.impl.Log;
import org.hibernate.datastore.ogm.orientdb.logging.impl.LoggerFactory;
import org.hibernate.ogm.model.key.spi.EntityKey;

/**
 * @author Sergey Chernolyas <sergey.chernolyas@gmail.com>
 */

public class EntityKeyUtil {

	private static final Log log = LoggerFactory.getLogger();

	public static void setFieldValue(StringBuilder queryBuffer, Object dbKeyValue) {
		if ( dbKeyValue != null ) {
			// @TODO not forget remove the code!
			log.debug( "dbKeyValue class:"+dbKeyValue+"; class :" + dbKeyValue.getClass() );
		}
		if ( dbKeyValue instanceof String || dbKeyValue instanceof UUID || dbKeyValue instanceof Character ) {
			queryBuffer.append( "'" ).append( dbKeyValue ).append( "'" );
		}
		else if ( dbKeyValue instanceof Date || dbKeyValue instanceof Calendar ) {
			Calendar calendar = null;
			if ( dbKeyValue instanceof Date ) {
				calendar = Calendar.getInstance();
				calendar.setTime( (Date) dbKeyValue );
			}
			else if ( dbKeyValue instanceof Calendar ) {
				calendar = (Calendar) dbKeyValue;
			}
			String formattedStr = ( new SimpleDateFormat( OrientDBConstant.DATETIME_FORMAT ) ).format( calendar.getTime() );
			queryBuffer.append( "'" ).append( formattedStr ).append( "'" );
		}
		else {
			queryBuffer.append( dbKeyValue );
		}
                queryBuffer.append(" ");
                
	}

	public static Object findPrimaryKeyValue(EntityKey key) {
		Object dbKeyValue = null;
		for ( int i = 0; i < key.getColumnNames().length; i++ ) {
			String columnName = key.getColumnNames()[i];
			Object columnValue = key.getColumnValues()[i];
			log.debug( "EntityKey: columnName: " + columnName + ";columnValue: " + columnValue + " (class:" + columnValue.getClass().getName() + ");" );
			if ( key.getMetadata().isKeyColumn( columnName ) ) {
				log.debug( "EntityKey: columnName: " + columnName + " is primary key!" );
				dbKeyValue = columnValue;
			}
		}
		return dbKeyValue;
	}

	public static String findPrimaryKeyName(EntityKey key) {
		for ( int i = 0; i < key.getColumnNames().length; i++ ) {
			String columnName = key.getColumnNames()[i];
			if ( key.getMetadata().isKeyColumn( columnName ) ) {
				log.debug( "EntityKey: columnName: " + columnName + " is primary key!" );
				return columnName;
			}
		}
		return null;
	}

	public static boolean existsPrimaryKeyInDB(Connection connection, EntityKey key) throws SQLException {
		String dbKeyName = key.getColumnNames()[0];
		Object dbKeyValue = key.getColumnValues()[0];

		boolean exists = false;
		Statement stmt = connection.createStatement();
		StringBuilder buffer = new StringBuilder( "select count(" + dbKeyName + ") from " );
		buffer.append( key.getTable() );
		buffer.append( " where " );
		buffer.append( dbKeyName );
		buffer.append( " = " );
		EntityKeyUtil.setFieldValue( buffer, dbKeyValue );
		log.debug( "existsPrimaryKeyInDB:Key:" + dbKeyName + " ; query:" + buffer.toString() );

		ResultSet rs = stmt.executeQuery( buffer.toString() );
		if ( rs.next() ) {
			long count = rs.getLong( 1 );
			log.debug( "existsPrimaryKeyInDB:Key:" + dbKeyName + " ; count:" + count );
			exists = count > 0;
		}
		return exists;
	}

	public static ORecordId findRid(Connection connection, String className, String businessKeyName, Object businessKeyValue) throws SQLException {
		log.debug( "findRid:className:" + className + " ; businessKeyName:" + businessKeyName + "; businessKeyValue:" + businessKeyValue );
		StringBuilder buffer = new StringBuilder( "select from " );
		buffer.append( className );
		buffer.append( " where " );
		buffer.append( businessKeyName );
		buffer.append( " = " );
		EntityKeyUtil.setFieldValue( buffer, businessKeyValue );
		log.debug( "findRid:className:" + buffer.toString() );
		ORecordId rid = null;
		ResultSet rs = connection.createStatement().executeQuery( buffer.toString() );
		if ( rs.next() ) {
			log.debug( "findRid: find" );
			rid = (ORecordId) rs.getObject( OrientDBConstant.SYSTEM_RID );
			log.debug( "findRid: rid: " + rid );
		}
		return rid;
	}
}
