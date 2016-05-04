/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.datastore.ogm.orientdb.query.impl;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author Sergey Chernolyas <sergey.chernolyas@gmail.com>
 */
public class CharacterParamValueSetter implements ParamValueSetter<Character> {

	@Override
	public void setValue(PreparedStatement preparedStatement, int index, Character value) throws SQLException {
		preparedStatement.setString( index, value.toString() );
	}

}
