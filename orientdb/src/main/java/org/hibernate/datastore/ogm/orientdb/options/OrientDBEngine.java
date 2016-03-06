package org.hibernate.datastore.ogm.orientdb.options;

/**
 * @author cristhiank on 3/6/16 (calovi86@gmail.com)
 */
public enum OrientDBEngine {
    PLOCAL("plocal"), MEMORY("memory"), REMOTE("remote");

    private final String name;

    OrientDBEngine(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
