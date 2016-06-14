/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.orientdb.query.impl;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;

/**
 * Setter for 'timestamp' value
 *
 * @author Sergey Chernolyas &lt;sergey.chernolyas@gmail.com&gt;
 */
public class TimestampParamValueSetter implements ParamValueSetter<Date> {

	@Override
	public void setValue(PreparedStatement preparedStatement, int index, Date value) throws SQLException {
		preparedStatement.setTimestamp( index, new Timestamp( value.getTime() ) );
	}

}
