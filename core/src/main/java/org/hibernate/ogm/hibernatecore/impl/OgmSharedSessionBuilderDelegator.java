/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.hibernatecore.impl;

import java.util.TimeZone;

import org.hibernate.FlushMode;
import org.hibernate.SessionBuilder;
import org.hibernate.SharedSessionBuilder;
import org.hibernate.engine.spi.AbstractDelegatingSharedSessionBuilder;
import org.hibernate.event.spi.EventSource;
import org.hibernate.ogm.OgmSession;
import org.hibernate.ogm.OgmSessionFactory;
import org.hibernate.resource.jdbc.spi.PhysicalConnectionHandlingMode;

/**
 * @author Emmanuel Bernard &lt;emmanuel@hibernate.org&gt;
 */
public class OgmSharedSessionBuilderDelegator extends AbstractDelegatingSharedSessionBuilder implements SharedSessionBuilder {
	private final SharedSessionBuilder builder;
	private final OgmSessionFactory factory;

	public OgmSharedSessionBuilderDelegator(SharedSessionBuilder builder, OgmSessionFactory factory) {
		super( builder );

		this.builder = builder;
		this.factory = factory;
	}

	@Override
	public OgmSession openSession() {
		return new OgmSessionImpl( factory, (EventSource) builder.openSession() );
	}

	@Override
	public SessionBuilder connectionHandlingMode(PhysicalConnectionHandlingMode mode) {
		return builder.connectionHandlingMode( mode );
	}

	@Override
	public SessionBuilder autoClear(boolean autoClear) {
		return builder.autoClear( autoClear );
	}

	@Override
	public SessionBuilder flushMode(FlushMode flushMode) {
		return builder.flushMode( flushMode );
	}

	@Override
	public SessionBuilder jdbcTimeZone(TimeZone timeZone) {
		return builder.jdbcTimeZone( timeZone );
	}

	@Override
	public SharedSessionBuilder flushMode() {
		return builder.flushMode();
	}
}
