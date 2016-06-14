/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.orientdb.query.impl;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * The interface defines contract for set value of query parameter
 *
 * @author Sergey Chernolyas &lt;sergey.chernolyas@gmail.com&gt;
 * @param <T> value's class
 */
public interface ParamValueSetter<T> {

	/**
	 * set value of parameter
	 *
	 * @param preparedStatement PreparedStatememt for setting parameter's value
	 * @param index index of parameter in query
	 * @param value value of the parameter
	 * @throws SQLException if any database error occurs
	 */
	void setValue(PreparedStatement preparedStatement, int index, T value) throws SQLException;

}
