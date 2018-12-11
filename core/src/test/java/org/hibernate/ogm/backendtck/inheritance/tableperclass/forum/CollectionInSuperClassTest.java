/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.backendtck.inheritance.tableperclass.forum;

import javax.persistence.Query;

import static org.fest.assertions.Assertions.assertThat;

import org.hibernate.ogm.utils.GridDialectType;
import org.hibernate.ogm.utils.SkipByGridDialect;
import org.hibernate.ogm.utils.TestForIssue;
import org.hibernate.ogm.utils.jpa.OgmJpaTestCase;
import org.junit.Test;

@TestForIssue(jiraKey = "OGM-1547")
@SkipByGridDialect(value = GridDialectType.MONGODB)
public class CollectionInSuperClassTest extends OgmJpaTestCase {

	@Test
	public void testFromUser() {
		EntityB b1 = new EntityB();
		b1.setName( "B1" );
		EntityB b2 = new EntityB();
		b2.setName( "B2" );

		EntityC c = new EntityC();
		c.setName( "C" );
		c.getEbs().add( b1 );
		c.getEbs().add( b2 );
		b1.setParent( c );
		b2.setParent( c );

		inTransaction( em -> {
			em.persist( c );
		} );

		inTransaction( em -> {
			Query q = em.createQuery( "SELECT c FROM EntityC c" );
			EntityC loaded = (EntityC) q.getSingleResult();

			assertThat( loaded ).isNotNull();
			assertThat( loaded.getName() ).isEqualTo( c.getName() );
			assertThat( loaded.getEbs() ).onProperty( "name" ).containsOnly( "B1", "B2" );
		} );
	}

	@Override
	protected Class<?>[] getAnnotatedClasses() {
		return new Class[]{ EntityA.class, EntityB.class, EntityC.class };
	}
}
