package org.hibernate.datastore.ogm.orientdb.configuration.impl;

import org.hibernate.datastore.ogm.orientdb.OrientDBProperties;
import org.hibernate.datastore.ogm.orientdb.options.OrientDBEngine;
import org.hibernate.ogm.cfg.spi.DocumentStoreConfiguration;
import org.hibernate.ogm.cfg.spi.Hosts;
import org.hibernate.ogm.options.spi.OptionsContext;
import org.hibernate.ogm.util.configurationreader.spi.ConfigurationPropertyReader;

/**
 * Configuration for {@link org.hibernate.datastore.ogm.orientdb.impl.OrientDBDatastoreProvider}.
 *
 * @author Cristhian Lopez
 */
public class OrientDBConfiguration extends DocumentStoreConfiguration {
    public static final int DEFAULT_HTTP_PORT = 2480;
    private static final String URL_SEPARATOR = ":";
    private static final String DB_SEPARATOR = "/";
    public static final int DEFAULT_BIN_PORT = 2424;
    private final ConfigurationPropertyReader propertyReader;

    public OrientDBConfiguration(ConfigurationPropertyReader propertyReader, OptionsContext globalOptions) {
        super(propertyReader, DEFAULT_BIN_PORT);
        this.propertyReader = propertyReader;
    }

    public String buildOrientDBUrl() {
        StringBuilder url = new StringBuilder();
        OrientDBEngine engine = propertyReader.property(OrientDBProperties.ENGINE, OrientDBEngine.class).withDefault(OrientDBEngine.REMOTE).getValue();
        Hosts.HostAndPort firstHost = getHosts().getFirst();
        url.append(engine.getName());
        url.append(URL_SEPARATOR);
        if (engine == OrientDBEngine.MEMORY || engine == OrientDBEngine.PLOCAL) {
            url.append(getDatabaseName());
        } else if (engine == OrientDBEngine.REMOTE) {
            url.append(firstHost.getHost());
            url.append(URL_SEPARATOR);
            url.append(firstHost.getPort());
            if (!getDatabaseName().startsWith(DB_SEPARATOR))
                url.append(DB_SEPARATOR);
            url.append(getDatabaseName());
        }
        return url.toString();
    }
}
