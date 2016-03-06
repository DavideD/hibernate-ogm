/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.datastore.ogm.orientdb.impl;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import org.hibernate.boot.registry.classloading.spi.ClassLoaderService;
import org.hibernate.datastore.ogm.orientdb.OrientDBDialect;
import org.hibernate.datastore.ogm.orientdb.configuration.impl.OrientDBConfiguration;
import org.hibernate.datastore.ogm.orientdb.logging.impl.Log;
import org.hibernate.datastore.ogm.orientdb.logging.impl.LoggerFactory;
import org.hibernate.ogm.datastore.spi.BaseDatastoreProvider;
import org.hibernate.ogm.datastore.spi.SchemaDefiner;
import org.hibernate.ogm.dialect.spi.GridDialect;
import org.hibernate.ogm.options.spi.OptionsService;
import org.hibernate.ogm.util.configurationreader.spi.ConfigurationPropertyReader;
import org.hibernate.service.spi.*;

import java.util.Map;

/**
 * @author chernolyassv
 * @author cristhiank (calovi86 at gmail.com)
 */
public class OrientDBDatastoreProvider extends BaseDatastoreProvider implements Startable, Stoppable, Configurable, ServiceRegistryAwareService {

    private static Log LOG = LoggerFactory.getLogger();
    private ODatabaseDocumentTx orientdb;
    private OrientDBConfiguration config;
    private ServiceRegistryImplementor serviceRegistry;

    @Override
    public Class<? extends GridDialect> getDefaultDialect() {
        return OrientDBDialect.class;
    }

    @Override
    public void start() {
        LOG.info("start");
        try {
            if (orientdb == null) {
                orientdb = createOrientDbConnection(config);
            }
        } catch (Exception e) {
            throw LOG.unableToStartDatastoreProvider(e);
        }
    }

    private ODatabaseDocumentTx createOrientDbConnection(OrientDBConfiguration config) {
        ODatabaseDocumentTx connection = new ODatabaseDocumentTx(config.buildOrientDBUrl());
        connection.open(config.getUsername(), config.getPassword());
        return connection;
    }

    public ODatabaseDocumentTx getConnection() {
        return orientdb;
    }

    @Override
    public void stop() {
        LOG.info("stop");
        if (!orientdb.isClosed()) {
            orientdb.close();
        }
    }

    @Override
    public void configure(Map configurationValues) {
        LOG.info("config map:" + configurationValues.toString());
        OptionsService optionsService = serviceRegistry.getService(OptionsService.class);
        ClassLoaderService classLoaderService = serviceRegistry.getService(ClassLoaderService.class);
        ConfigurationPropertyReader propertyReader = new ConfigurationPropertyReader(configurationValues, classLoaderService);
        this.config = new OrientDBConfiguration(propertyReader, optionsService.context().getGlobalOptions());

    }

    @Override
    public void injectServices(ServiceRegistryImplementor serviceRegistry) {
        this.serviceRegistry = serviceRegistry;
    }

    @Override
    public Class<? extends SchemaDefiner> getSchemaDefinerType() {
        LOG.info("getSchemaDefinerType");
        return super.getSchemaDefinerType();
    }

}
