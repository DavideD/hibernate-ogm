/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.datastore.ogm.orientdb.impl;

import com.tinkerpop.blueprints.impls.orient.OrientGraph;
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

	private static Log log = LoggerFactory.getLogger();
	private OrientGraph orientdb;
	private OrientDBConfiguration config;
	private ServiceRegistryImplementor serviceRegistry;

	@Override
	public Class<? extends GridDialect> getDefaultDialect() {
		return OrientDBDialect.class;
	}

	@Override
	public void start() {
		try {
			if ( orientdb == null ) {
				orientdb = createOrientDbConnection( config );
			}
		}
		catch (Exception e) {
			throw log.unableToStartDatastoreProvider( e );
		}
	}

	private OrientGraph createOrientDbConnection(OrientDBConfiguration config) {
		OrientGraph graph = new OrientGraph( config.buildOrientDBUrl(), false );
		//Disable Blueprints standard validations
		graph.setStandardElementConstraints( false );
		return graph;
	}

	public OrientGraph getConnection() {
		return orientdb;
	}

	@Override
	public void stop() {
		if ( !orientdb.isClosed() ) {
			orientdb.shutdown();
		}
	}

	@Override
	public void configure(Map configurationValues) {
		OptionsService optionsService = serviceRegistry.getService( OptionsService.class );
		ClassLoaderService classLoaderService = serviceRegistry.getService( ClassLoaderService.class );
		ConfigurationPropertyReader propertyReader = new ConfigurationPropertyReader( configurationValues, classLoaderService );
		this.config = new OrientDBConfiguration( propertyReader, optionsService.context().getGlobalOptions() );

	}

	@Override
	public void injectServices(ServiceRegistryImplementor serviceRegistry) {
		this.serviceRegistry = serviceRegistry;
	}

	@Override
	public Class<? extends SchemaDefiner> getSchemaDefinerType() {
		return super.getSchemaDefinerType();
	}

}
