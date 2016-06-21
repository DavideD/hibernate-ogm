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
 * Setter for 'byte' value
 *
 * @author Sergey Chernolyas &lt;sergey.chernolyas@gmail.com&gt;
 */
public class ByteParamValueSetter implements ParamValueSetter<Byte> {

	@Override
	public void setValue(PreparedStatement preparedStatement, int index, Byte value) throws SQLException {
		preparedStatement.setByte( index, value );
	}

}
