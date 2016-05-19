/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.datastore.ogm.orientdb.utils;

import com.orientechnologies.common.exception.OException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.hibernate.datastore.ogm.orientdb.logging.impl.Log;
import org.hibernate.datastore.ogm.orientdb.logging.impl.LoggerFactory;

/**
 * @author Sergey Chernolyas &lt;sergey.chernolyas@gmail.com&gt;
 */
public class SequenceUtil {

	private static final Log log = LoggerFactory.getLogger();

	public static long getNextSequenceValue(Connection connection, String seqName) {
		long nextValue = 0;
		String query = String.format( "select sequence('%s').next()", seqName );
		try {
			Statement stmt = connection.createStatement();
			ResultSet rs = stmt.executeQuery( query );
			if ( rs.next() ) {
				nextValue = rs.getLong( "sequence" );
			}
		}
		catch (SQLException | OException sqle) {
			throw log.cannotExecuteQuery( query, sqle );
		}
		return nextValue;
	}

	public static long getNextTableValue(Connection connection, String seqTable, String pkColumnName, String pkColumnValue, String valueColumnName,
			int initValue, int inc) {
		long nextValue = 0;
		String query = String.format( "select getTableSeqValue('%s','%s','%s','%s',%d,%d) as %s ",
				seqTable, pkColumnName, pkColumnValue, valueColumnName, initValue, inc, valueColumnName );
		log.debugf( "table sequence query: %s ", query );
		try {
			Statement stmt = connection.createStatement();
			ResultSet rs = stmt.executeQuery( query );
			if ( rs.next() ) {
				nextValue = rs.getLong( valueColumnName );
			}
		}
		catch (SQLException | OException sqle) {
			log.error( "Error!", sqle );
			throw log.cannotExecuteStoredProcedure( "getTableSeqValue", sqle );
		}
		log.debugf( "nextValue %d ", nextValue );
		return nextValue;
	}
}
