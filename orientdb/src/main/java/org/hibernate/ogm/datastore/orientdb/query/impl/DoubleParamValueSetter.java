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
 * Setter for 'double' value
 *
 * @author Sergey Chernolyas &lt;sergey.chernolyas@gmail.com&gt;
 */
public class DoubleParamValueSetter implements ParamValueSetter<Double> {

	@Override
	public void setValue(PreparedStatement preparedStatement, int index, Double value) throws SQLException {
		preparedStatement.setDouble( index, value );
	}

}
