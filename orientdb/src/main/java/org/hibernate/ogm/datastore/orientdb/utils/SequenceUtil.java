/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.orientdb.utils;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.function.OFunction;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.util.List;

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
	 */
	public static long getNextSequenceValue(ODatabaseDocumentTx db, String seqName) {
		String query = String.format( "select sequence('%s').next()", seqName );
		List<ODocument> documents = NativeQueryUtil.executeIdempotentQuery( db, query );
		return documents.get( 0 ).field( "sequence", Long.class );
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

	public static long getNextTableValue(ODatabaseDocumentTx db, String seqTable, String pkColumnName, String pkColumnValue, String valueColumnName,
			int initValue, int inc) {
		OFunction executeQuery = db.getMetadata().getFunctionLibrary().getFunction( "getTableSeqValue".toUpperCase() );
		List<ODocument> documents = (List<ODocument>) executeQuery.execute( seqTable, pkColumnName, pkColumnValue, valueColumnName, initValue, inc,
				valueColumnName );
		return documents.get( 0 ).field( valueColumnName, Long.class );
	}
}