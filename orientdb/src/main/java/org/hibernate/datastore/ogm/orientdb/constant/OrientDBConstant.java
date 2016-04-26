/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */

package org.hibernate.datastore.ogm.orientdb.constant;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author Sergey Chernolyas <sergey.chernolyas@gmail.com>
 */

public class OrientDBConstant {

	public static final String DATE_FORMAT = "yyyy-MM-dd'Z'";
	// public static final String DATETIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
	public static final String DATETIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'";
	public static final Set<String> LINK_FIELDS;
	public static final Set<String> SYSTEM_FIELDS;

	public static final String SYSTEM_RID = "@rid";
	public static final String SYSTEM_VERSION = "@version";
	public static final String SYSTEM_CLASS = "@class";
	public static final Map<String, String> MAPPING_FIELDS;
	public static final Set<Class> BASE64_TYPES;
	public static final String NULL_VALUE = "null";
	public static final Set<String> SYSTEM_CLASS_SET;
	public static final String HIBERNATE_SEQUENCE = "hibernate_sequence";
	public static final String HIBERNATE_SEQUENCE_TABLE = "sequences";

	static {
		Set<String> set = new HashSet<>();
		set.add( SYSTEM_RID );
		set.add( SYSTEM_VERSION );
		set.add( SYSTEM_CLASS );
		SYSTEM_FIELDS = Collections.unmodifiableSet( set );
		LINK_FIELDS = Collections.unmodifiableSet( new HashSet<String>( Arrays.asList( new String[]{ "in_", "out_" } ) ) );
		Map<String, String> map = new HashMap<>();
		map.put( "version", "@version" );
		MAPPING_FIELDS = Collections.unmodifiableMap( map );

		Set<Class> set1 = new HashSet<>();
		set1.add( BigInteger.class );
		set1.add( byte[].class );
		BASE64_TYPES = Collections.unmodifiableSet( set1 );

		SYSTEM_CLASS_SET = Collections
				.unmodifiableSet( new HashSet<String>( Arrays.asList( "V", "OSequence", "ORestricted", "OTriggered", "OIdentity", "ORole", "OSchedule",
						"OUser", "OFunction", "E" ) ) );

	}
}
