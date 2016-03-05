/*
<<<<<<< HEAD
* Hibernate OGM, Domain model persistence for NoSQL datastores
* 
* License: GNU Lesser General Public License (LGPL), version 2.1 or later
* See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
*/

package org.hibernate.datastore.ogm.orientdb.utils;

import com.orientechnologies.orient.core.id.ORecordId;
=======
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */

package org.hibernate.datastore.ogm.orientdb.utils;

>>>>>>> 3712b2f73e6a708158478452211328c54279a26c
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
<<<<<<< HEAD
import java.util.UUID;
=======
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.UUID;

>>>>>>> 3712b2f73e6a708158478452211328c54279a26c
import org.hibernate.datastore.ogm.orientdb.constant.OrientDBConstant;
import org.hibernate.datastore.ogm.orientdb.logging.impl.Log;
import org.hibernate.datastore.ogm.orientdb.logging.impl.LoggerFactory;
import org.hibernate.ogm.model.key.spi.EntityKey;

<<<<<<< HEAD
=======
import com.orientechnologies.orient.core.id.ORecordId;

>>>>>>> 3712b2f73e6a708158478452211328c54279a26c
/**
 * @author Sergey Chernolyas <sergey.chernolyas@gmail.com>
 */

public class EntityKeyUtil {

	private static final Log log = LoggerFactory.getLogger();

	public static void setFieldValue(StringBuilder queryBuffer, Object dbKeyValue) {
<<<<<<< HEAD
		if ( dbKeyValue instanceof String || dbKeyValue instanceof UUID ) {
			queryBuffer.append( "'" ).append( dbKeyValue ).append( "'" );
		}
		else {
			queryBuffer.append( dbKeyValue );
		}
	}
        
=======
		if ( dbKeyValue != null ) {
			// @TODO not forget remove the code!
			log.debug( "dbKeyValue class:" + dbKeyValue + "; class :" + dbKeyValue.getClass() );
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
>>>>>>> 3712b2f73e6a708158478452211328c54279a26c

	public static Object findPrimaryKeyValue(EntityKey key) {
		Object dbKeyValue = null;
		for ( int i = 0; i < key.getColumnNames().length; i++ ) {
			String columnName = key.getColumnNames()[i];
			Object columnValue = key.getColumnValues()[i];
<<<<<<< HEAD
			log.info( "EntityKey: columnName: " + columnName + ";columnValue: " + columnValue + " (class:" + columnValue.getClass().getName() + ");" );
			if ( key.getMetadata().isKeyColumn( columnName ) ) {
				log.info( "EntityKey: columnName: " + columnName + " is primary key!" );
=======
			log.debug( "EntityKey: columnName: " + columnName + ";columnValue: " + columnValue + " (class:" + columnValue.getClass().getName() + ");" );
			if ( key.getMetadata().isKeyColumn( columnName ) ) {
				log.debug( "EntityKey: columnName: " + columnName + " is primary key!" );
>>>>>>> 3712b2f73e6a708158478452211328c54279a26c
				dbKeyValue = columnValue;
			}
		}
		return dbKeyValue;
	}

	public static String findPrimaryKeyName(EntityKey key) {
		for ( int i = 0; i < key.getColumnNames().length; i++ ) {
			String columnName = key.getColumnNames()[i];
			if ( key.getMetadata().isKeyColumn( columnName ) ) {
<<<<<<< HEAD
				log.info( "EntityKey: columnName: " + columnName + " is primary key!" );
=======
				log.debug( "EntityKey: columnName: " + columnName + " is primary key!" );
>>>>>>> 3712b2f73e6a708158478452211328c54279a26c
				return columnName;
			}
		}
		return null;
	}

<<<<<<< HEAD
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
		log.info( "existsPrimaryKeyInDB:Key:" + dbKeyName + " ; query:" + buffer.toString() );

		ResultSet rs = stmt.executeQuery( buffer.toString() );
		if ( rs.next() ) {
			long count = rs.getLong( 1 );
			log.info( "existsPrimaryKeyInDB:Key:" + dbKeyName + " ; count:" + count );
			exists = count > 0;
=======
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
				log.debug( "existsPrimaryKeyInDB:Key:" + dbKeyName + " ; count:" + count );
				exists = count > 0;
			}
		}
		catch (SQLException sqle) {
			throw log.cannotExecuteQuery( buffer.toString(), sqle );

>>>>>>> 3712b2f73e6a708158478452211328c54279a26c
		}
		return exists;
	}

<<<<<<< HEAD
	public static ORecordId findRid(Connection connection, String className, String businessKeyName, Object businessKeyValue) throws SQLException {
		log.info( "findRid:className:" + className + " ; businessKeyName:" + businessKeyName + "; businessKeyValue:" + businessKeyValue );
		StringBuilder buffer = new StringBuilder( "select from " );
		buffer.append( className );
		buffer.append( " where " );
		buffer.append( businessKeyName );
		buffer.append( " = " );
		EntityKeyUtil.setFieldValue( buffer, businessKeyValue );
		log.info( "findRid:className:" + buffer.toString() );
		ORecordId rid = null;
		ResultSet rs = connection.createStatement().executeQuery( buffer.toString() );
		if ( rs.next() ) {
			log.info( "findRid: find" );
			rid = (ORecordId) rs.getObject( OrientDBConstant.SYSTEM_RID );
=======
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
>>>>>>> 3712b2f73e6a708158478452211328c54279a26c
		}
		return rid;
	}
}
