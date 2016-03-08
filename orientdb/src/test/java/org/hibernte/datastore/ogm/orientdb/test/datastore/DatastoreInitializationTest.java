/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernte.datastore.ogm.orientdb.test.datastore;

import org.hibernate.ogm.cfg.OgmProperties;
import org.hibernate.ogm.datastore.spi.DatastoreProvider;
import org.hibernate.ogm.utils.SkippableTestRunner;
import org.hibernate.ogm.utils.TestHelper;
import org.hibernate.service.spi.ServiceException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.internal.matchers.ThrowableMessageMatcher.hasMessage;

/**
 * @author cristhiank on 3/6/16 (calovi86 at gmail.com).
 */
@RunWith(SkippableTestRunner.class)
public class DatastoreInitializationTest {

	private static final String NON_EXISTENT_IP = "203.0.113.1";

	@Rule
	public ExpectedException error = ExpectedException.none();

	private Map<String, Object> cfg;

	@Before
	public void setUp() {
		cfg = new HashMap<String, Object>();
		cfg.put( OgmProperties.HOST, NON_EXISTENT_IP );
		cfg.put( OgmProperties.DATABASE, "orientdb" );
		cfg.put( OgmProperties.USERNAME, "root" );
		cfg.put( OgmProperties.PASSWORD, "toor" );
	}

	@Test
	public void testConnectionErrorWrappedInHibernateException() throws Exception {
		error.expect( ServiceException.class );
		error.expectMessage( "OGM000071" );
		//nested exception
		error.expectCause( hasMessage( containsString( "OGM001214" ) ) );

		// will start the service
		TestHelper.getDefaultTestStandardServiceRegistry( cfg ).getService( DatastoreProvider.class );
	}

	@Test
	public void testConnectionRefused() {
		error.expect( ServiceException.class );
		error.expectMessage( "OGM000071" );
		// the timeout exception thrown by the driver will actually contain some information about the authentication
		// error. Obviously quite fragile. Might change
		error.expectCause( hasMessage( containsString( "Connection refused" ) ) );

		// will start the service
		TestHelper.getDefaultTestStandardServiceRegistry( cfg ).getService( DatastoreProvider.class );
	}
}
