/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.orientdb.query.impl;

import java.sql.SQLException;

/**
 * Setter for 'integer' value
 *
 * @author Sergey Chernolyas &lt;sergey.chernolyas@gmail.com&gt;
 */
public class IntegerParamValueSetter implements ParamValueSetter<Integer> {

	@Override
	public void setValue(int index, Integer value) throws SQLException {
		// preparedStatement.setInt( index, value );
	}

}