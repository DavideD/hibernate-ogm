<<<<<<< HEAD
 /*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 * 
=======
/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
>>>>>>> 3712b2f73e6a708158478452211328c54279a26c
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */

package org.hibernate.datastore.ogm.orientdb.utils;

import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;

/**
<<<<<<< HEAD
 *
 * @author Sergey Chernolyas <sergey.chernolyas@gmail.com>
 */


public class ORidBagUtil {
    
     public static String convertORidBagToString(ORidBag t) {
        StringBuilder builder = new StringBuilder();
        t.toStream(builder);
        return builder.toString();
    }

    
    public static ORidBag convertStringToORidBag(String string) {
        ORidBag t = new ORidBag();
        t.fromStream(new StringBuilder(string));
        return t;
    }
    
=======
 * @author Sergey Chernolyas <sergey.chernolyas@gmail.com>
 */

public class ORidBagUtil {

	public static String convertORidBagToString(ORidBag t) {
		StringBuilder builder = new StringBuilder();
		t.toStream( builder );
		return builder.toString();
	}

	public static ORidBag convertStringToORidBag(String string) {
		ORidBag t = new ORidBag();
		t.fromStream( new StringBuilder( string ) );
		return t;
	}
>>>>>>> 3712b2f73e6a708158478452211328c54279a26c
}
