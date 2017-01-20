/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.service.impl;

import java.util.List;

import org.hibernate.SessionFactory;
import org.hibernate.boot.spi.SessionFactoryOptions;
import org.hibernate.engine.config.spi.ConfigurationService;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.service.Service;
import org.hibernate.service.internal.ProvidedService;
import org.hibernate.service.internal.SessionFactoryServiceRegistryImpl;
import org.hibernate.service.spi.Configurable;
import org.hibernate.service.spi.ServiceBinding;
import org.hibernate.service.spi.ServiceRegistryImplementor;
import org.hibernate.service.spi.SessionFactoryServiceInitiator;

/**
 * Bind services requiring a {@link SessionFactory}.
 *
 * @author Davide D'Alto &lt;davide@hibernate.org&gt;
 */
public class OgmSessionFactoryServiceRegistryImpl extends SessionFactoryServiceRegistryImpl {

	public OgmSessionFactoryServiceRegistryImpl(ServiceRegistryImplementor parent,
			List<SessionFactoryServiceInitiator> initiators,
			List<ProvidedService> providedServices,
			SessionFactoryImplementor sessionFactory,
			SessionFactoryOptions sessionFactoryOptions) {
		super( parent, initiators, providedServices, sessionFactory, sessionFactoryOptions );
	}

	@Override
	protected <R extends Service> void registerService(ServiceBinding<R> serviceBinding, R service) {
		if ( service != null ) {
			super.registerService( serviceBinding, service );
		}
		else {
			System.out.println( "Null" );
		}
	}

	@Override
	public <R extends Service> void configureService(ServiceBinding<R> serviceBinding) {
		if ( Configurable.class.isInstance( serviceBinding.getService() ) ) {
			( (Configurable) serviceBinding.getService() ).configure( getService( ConfigurationService.class ).getSettings() );
		}
	}
}
