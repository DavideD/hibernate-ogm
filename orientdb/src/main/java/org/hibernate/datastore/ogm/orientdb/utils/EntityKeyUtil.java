/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */

package org.hibernate.datastore.ogm.orientdb.utils;

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
import org.hibernate.mapping.Column;
import org.hibernate.ogm.model.key.spi.EntityKey;

import com.orientechnologies.orient.core.id.ORecordId;

/**
 * @author Sergey Chernolyas <sergey.chernolyas@gmail.com>
 */

public class EntityKeyUtil {

	private static final Log log = LoggerFactory.getLogger();

	public static boolean isEmbeddedColumn(Column column) {
		return isEmbeddedColumn( column.getName() );
	}

	public static boolean isEmbeddedColumn(String column) {
		return column.contains( "." );
	}

	public static void setFieldValue(StringBuilder queryBuffer, Object dbKeyValue) {
		if ( dbKeyValue != null ) {
			// @TODO not forget remove the code!
			log.debugf( "dbKeyValue class: %s ; class: %s ", dbKeyValue, dbKeyValue.getClass() );
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
		queryBuffer.append( " " );

	}

	public static Object findPrimaryKeyValue(EntityKey key) {
		Object dbKeyValue = null;
		for ( int i = 0; i < key.getColumnNames().length; i++ ) {
			String columnName = key.getColumnNames()[i];
			Object columnValue = key.getColumnValues()[i];
			log.debugf( "EntityKey: columnName: %s ;columnValue: %s (class:%s)", columnName, columnValue, columnValue.getClass().getName() );
			if ( key.getMetadata().isKeyColumn( columnName ) ) {
				log.debugf( "EntityKey: columnName: %s is primary key!", columnName );
				dbKeyValue = columnValue;
			}
		}
		return dbKeyValue;
	}

	public static String findPrimaryKeyName(EntityKey key) {
		for ( int i = 0; i < key.getColumnNames().length; i++ ) {
			String columnName = key.getColumnNames()[i];
			if ( key.getMetadata().isKeyColumn( columnName ) ) {
				log.debugf( "EntityKey: columnName: %s is primary key!", columnName );
				return columnName;
			}
		}
		return null;
	}

	public static boolean existsPrimaryKeyInDB(Connection connection, EntityKey key) {
		String dbKeyName = key.getColumnNames()[0];
		Object dbKeyValue = key.getColumnValues()[0];
		boolean exists = false;
		StringBuilder buffer = new StringBuilder();
		try {
			Statement stmt = connection.createStatement();
			buffer.append( "select count(" ).append( dbKeyName ).append( ") from " );
			buffer.append( key.getTable() ).append( " where " ).append( dbKeyName );
			buffer.append( " = " );
			EntityKeyUtil.setFieldValue( buffer, dbKeyValue );
			ResultSet rs = stmt.executeQuery( buffer.toString() );
			if ( rs.next() ) {
				long count = rs.getLong( 1 );
				log.debugf( "existsPrimaryKeyInDB:Key: %s ; count: %d", dbKeyName, count );
				exists = count > 0;
			}
		}
		catch (SQLException sqle) {
			throw log.cannotExecuteQuery( buffer.toString(), sqle );

		}
		return exists;
	}

	public static ORecordId findRid(Connection connection, String className, String businessKeyName, Object businessKeyValue) {
		StringBuilder buffer = new StringBuilder();
		ORecordId rid = null;
		try {
			log.debug( "findRid:className:" + className + " ; businessKeyName:" + businessKeyName + "; businessKeyValue:" + businessKeyValue );
			buffer.append( "select from " ).append( className ).append( " where " );
			buffer.append( businessKeyName ).append( " = " );
			EntityKeyUtil.setFieldValue( buffer, businessKeyValue );

			ResultSet rs = connection.createStatement().executeQuery( buffer.toString() );
			if ( rs.next() ) {
				rid = (ORecordId) rs.getObject( OrientDBConstant.SYSTEM_RID );
			}
		}
		catch (SQLException sqle) {
			throw log.cannotExecuteQuery( buffer.toString(), sqle );
		}
		return rid;
	}
}
