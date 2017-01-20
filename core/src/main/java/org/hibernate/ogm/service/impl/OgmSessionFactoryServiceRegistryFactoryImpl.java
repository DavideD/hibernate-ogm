/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.service.impl;

import org.hibernate.boot.registry.classloading.spi.ClassLoaderService;
import org.hibernate.boot.spi.SessionFactoryOptions;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.service.internal.SessionFactoryServiceRegistryBuilderImpl;
import org.hibernate.service.spi.ServiceRegistryImplementor;
import org.hibernate.service.spi.SessionFactoryServiceContributor;
import org.hibernate.service.spi.SessionFactoryServiceInitiator;
import org.hibernate.service.spi.SessionFactoryServiceRegistry;
import org.hibernate.service.spi.SessionFactoryServiceRegistryFactory;

/**
 * Factory for the creation of {@link OgmSessionFactoryServiceRegistryImpl}.
 *
 * @author Davide D'Alto &lt;davide@hibernate.org&gt;
 */
public class OgmSessionFactoryServiceRegistryFactoryImpl implements SessionFactoryServiceRegistryFactory {

	private final ServiceRegistryImplementor theBasicServiceRegistry;
	private SessionFactoryServiceRegistry ogmSessionFactoryServiceRegistryImpl;

	public OgmSessionFactoryServiceRegistryFactoryImpl(ServiceRegistryImplementor theBasicServiceRegistry) {
		this.theBasicServiceRegistry = theBasicServiceRegistry;
	}

	@Override
	public SessionFactoryServiceRegistry buildServiceRegistry(
			SessionFactoryImplementor sessionFactory,
			SessionFactoryOptions options) {
		if ( ogmSessionFactoryServiceRegistryImpl == null ) {
			final ClassLoaderService cls = options.getServiceRegistry().getService( ClassLoaderService.class );
			final SessionFactoryServiceRegistryBuilderImpl builder = new SessionFactoryServiceRegistryBuilderImpl( theBasicServiceRegistry );

			for ( SessionFactoryServiceContributor contributor : cls.loadJavaServices( SessionFactoryServiceContributor.class ) ) {
				contributor.contribute( builder );
			}

			for ( SessionFactoryServiceInitiator initiator : OgmSessionFactoryServiceInitiators.LIST ) {
				builder.addInitiator( initiator );
			}
			ogmSessionFactoryServiceRegistryImpl = builder.buildSessionFactoryServiceRegistry( sessionFactory, options );
		}
		return ogmSessionFactoryServiceRegistryImpl;
	}
}
