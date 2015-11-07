/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.redis.test.mapping;

import org.hibernate.ogm.OgmSession;
import org.hibernate.ogm.datastore.redis.test.RedisOgmTestCase;
import org.hibernate.ogm.utils.GridDialectType;
import org.hibernate.ogm.utils.SkipByGridDialect;

import org.json.JSONException;
import org.junit.Before;
import org.junit.Test;
import org.skyscreamer.jsonassert.JSONAssert;
import org.skyscreamer.jsonassert.JSONCompareMode;

/**
 * Test for Redis JSON mapping.
 *
 * @author Mark Paluch
 */
@SkipByGridDialect(GridDialectType.REDIS_HASH)
public class RedisJsonMappingTest extends RedisOgmTestCase {

	@Before
	public void before() throws Exception {
		getConnection().flushall();
	}

	@Test
	public void verifyRedisRepresentation() throws JSONException {
		OgmSession session = openSession();
		session.getTransaction().begin();

		// given
		Donut donut = new Donut( "homers-donut", 7.5, Donut.Glaze.Pink, "pink-donut" );
		session.persist( donut );

		session.getTransaction().commit();

		// when
		String representation = new String( getConnection().get( "Donut:homers-donut") );

		// then
		JSONAssert.assertEquals( "{'alias':'pink-donut','radius':7.5,'glaze':2}", representation, JSONCompareMode.STRICT );

		session.close();
	}

	@Override
	protected Class<?>[] getAnnotatedClasses() {
		return new Class<?>[] {Family.class, Plant.class, Donut.class};
	}
}
