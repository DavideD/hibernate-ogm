/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.orientdb.utils;

import com.orientechnologies.common.exception.OException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.hibernate.HibernateException;
import org.hibernate.ogm.datastore.orientdb.logging.impl.Log;
import org.hibernate.ogm.datastore.orientdb.logging.impl.LoggerFactory;

/**
 * Utility class for working with sequences
 *
 * @author Sergey Chernolyas &lt;sergey.chernolyas@gmail.com&gt;
 */
public class SequenceUtil {

	private static final Log log = LoggerFactory.getLogger();

	/**
	 * Get next value from sequence
	 *
	 * @param connection connection to OrientDB
	 * @param seqName name of sequence
	 * @return next value of the sequence
	 * @throws HibernateException if {@link SQLException} or {@link OException} occurs
	 */
	public static long getNextSequenceValue(Connection connection, String seqName) {
		long nextValue = -1;
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

	/**
	 * Get next value from table generator. Stored procedure 'getTableSeqValue' uses for generate value
	 *
	 * @param connection connection to OrientDB
	 * @param seqTable name of table that uses for generate values
	 * @param pkColumnName name of column that contains name of sequence
	 * @param pkColumnValue value of name of sequence
	 * @param valueColumnName name of column that contains value of sequence
	 * @param initValue initial value
	 * @param inc value of increment
	 * @return next value of the sequence
	 * @throws HibernateException if {@link SQLException} or {@link OException} occurs
	 */
	public static long getNextTableValue(Connection connection, String seqTable, String pkColumnName, String pkColumnValue, String valueColumnName,
			int initValue, int inc) {
		long nextValue = -1;
		String query = String.format( "select getTableSeqValue('%s','%s','%s','%s',%d,%d) as %s ",
				seqTable, pkColumnName, pkColumnValue, valueColumnName, initValue, inc, valueColumnName );
		try {
			Statement stmt = connection.createStatement();
			ResultSet rs = stmt.executeQuery( query );
			if ( rs.next() ) {
				nextValue = rs.getLong( valueColumnName );
			}
		}
		catch (SQLException | OException sqle) {
			throw log.cannotExecuteStoredProcedure( "getTableSeqValue", sqle );
		}
		return nextValue;
	}
}
