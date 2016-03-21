/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.utils;

import org.hibernate.ogm.dialect.spi.GridDialect;

public class SkipByHelper {

	public static boolean skipForGridDialect(GridDialectType requiredType) {
		Class<? extends GridDialect> currentGridDialectClass = TestHelper.getCurrentGridDialect();
		if ( requiredType == null || requiredType.loadGridDialectClass() == null ) {
			// can be used for all dialects
			return true;
		}
		return ( requiredType.loadGridDialectClass().isAssignableFrom( currentGridDialectClass ) );
	}

}
