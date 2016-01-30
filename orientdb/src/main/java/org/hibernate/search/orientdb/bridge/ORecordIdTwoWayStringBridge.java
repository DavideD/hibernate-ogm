/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.search.orientdb.bridge;

import com.orientechnologies.orient.core.id.ORecordId;
import org.hibernate.search.bridge.TwoWayStringBridge;

/**
 *
 * @author chernolyassv
 */
public class ORecordIdTwoWayStringBridge implements TwoWayStringBridge {

    @Override
    public Object stringToObject(String string) {
        System.out.println("ORecordIdTwoWayStringBridge: "+string);
        return new ORecordId(string);
    }

    @Override
    public String objectToString(Object o) {
        ORecordId id = (ORecordId) o;
        System.out.println("ORecordIdTwoWayStringBridge: "+id);
        return id.toString();
    }
    
}
