/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.orientdb.dialect.impl;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;

import org.hibernate.ogm.datastore.orientdb.constant.OrientDBConstant;
import org.hibernate.ogm.datastore.orientdb.logging.impl.Log;
import org.hibernate.ogm.datastore.orientdb.logging.impl.LoggerFactory;
import org.hibernate.HibernateException;
import org.hibernate.ogm.datastore.map.impl.MapTupleSnapshot;
import org.hibernate.ogm.dialect.query.spi.ClosableIterator;
import org.hibernate.ogm.model.spi.Tuple;

/**
 * Closable iterator through {@link ResultSet}
 *
 * @author Sergey Chernolyas &lt;sergey.chernolyas@gmail.com&gt;
 */
public class ResultSetTupleIterator implements ClosableIterator<Tuple> {

	private static final Log log = LoggerFactory.getLogger();

	private final ResultSet resultSet;

	public ResultSetTupleIterator(ResultSet resultSet) {
		this.resultSet = resultSet;
	}

	@Override
	public boolean hasNext() {
		try {
			return !resultSet.isLast();
		}
		catch (SQLException e) {
			throw log.cannotMoveOnResultSet( e );
		}
	}

	@Override
	public Tuple next() {
		try {
			resultSet.next();
			return convert();
		}
		catch (SQLException e) {
			throw log.cannotMoveOnResultSet( e );
		}
	}

	/**
	 * convert {@link ResultSet} to {@link Tuple}
	 *
	 * @return tuple from ResultSet
	 * @throws HibernateException if any database error occurs
	 */
	private Tuple convert() {
		Map<String, Object> map = new LinkedHashMap<>();
		try {
			ResultSetMetaData resultSetMetadata = resultSet.getMetaData();
			for ( int i = 0; i < resultSetMetadata.getColumnCount(); i++ ) {
				int fieldNum = i + 1;
				Object dbValue = resultSet.getObject( fieldNum );
				map.put( resultSetMetadata.getColumnName( fieldNum ), dbValue );
			}
			for ( String systemField : OrientDBConstant.SYSTEM_FIELDS ) {
				map.put( systemField, resultSet.getObject( systemField ) );
			}
		}
		catch (SQLException sqle) {
			throw log.cannotProcessResultSet( sqle );
		}
		return new Tuple( new MapTupleSnapshot( map ) );
	}

	@Override
	public void remove() {
		try {
			resultSet.deleteRow();
		}
		catch (SQLException e) {
			throw log.cannotDeleteRowFromResultSet( e );
		}
	}

	@Override
	public void close() {
		try {
			resultSet.close();
		}
		catch (SQLException e) {
			throw log.cannotCloseResultSet( e );
		}
	}

}
