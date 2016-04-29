/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.datastore.ogm.orientdb.jpa;

import java.io.Serializable;

/**
 *
 * @author Sergey Chernolyas <sergey.chernolyas@gmail.com>
 */

public class PassportPK implements Serializable {
    private int seria;
    private long number;

    public int getSeria() {
        return seria;
    }

    public void setSeria(int seria) {
        this.seria = seria;
    }

    public long getNumber() {
        return number;
    }

    public void setNumber(long number) {
        this.number = number;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 23 * hash + this.seria;
        hash = 23 * hash + (int) (this.number ^ (this.number >>> 32));
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final PassportPK other = (PassportPK) obj;
        if (this.seria != other.seria) {
            return false;
        }
        if (this.number != other.number) {
            return false;
        }
        return true;
    }
    
}
