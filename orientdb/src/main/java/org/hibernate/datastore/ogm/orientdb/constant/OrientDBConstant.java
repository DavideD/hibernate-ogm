/*
<<<<<<< HEAD
* Hibernate OGM, Domain model persistence for NoSQL datastores
* 
* License: GNU Lesser General Public License (LGPL), version 2.1 or later
* See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
*/
=======
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
>>>>>>> 3712b2f73e6a708158478452211328c54279a26c

package org.hibernate.datastore.ogm.orientdb.constant;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * @author Sergey Chernolyas <sergey.chernolyas@gmail.com>
 */

public class OrientDBConstant {

<<<<<<< HEAD
	public static final String SYSTEM_VERSION = "@version";
	public static final String SYSTEM_RID = "@rid";

	public static final Set<String> SYSTEM_FIELDS;
        public static final Set<String> LINK_FIELDS;
=======
	public static final String DATE_FORMAT = "yyyy-MM-dd'Z'";
	public static final String DATETIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
	public static final Set<String> LINK_FIELDS;
	public static final Set<String> SYSTEM_FIELDS;

	public static final String SYSTEM_RID = "@rid";
	public static final String SYSTEM_VERSION = "@version";
>>>>>>> 3712b2f73e6a708158478452211328c54279a26c

	static {
		Set<String> set = new HashSet<>();
		set.add( SYSTEM_RID );
		set.add( SYSTEM_VERSION );
		SYSTEM_FIELDS = Collections.unmodifiableSet( set );
<<<<<<< HEAD
                LINK_FIELDS = Collections.unmodifiableSet(new HashSet<String>(Arrays.asList(new String[]{"in_","out_"})));
=======
		LINK_FIELDS = Collections.unmodifiableSet( new HashSet<String>( Arrays.asList( new String[]{ "in_", "out_" } ) ) );
>>>>>>> 3712b2f73e6a708158478452211328c54279a26c
	}
}
