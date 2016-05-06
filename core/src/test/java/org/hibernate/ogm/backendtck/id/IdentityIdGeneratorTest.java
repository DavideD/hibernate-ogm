/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.backendtck.id;

import static org.fest.assertions.Assertions.assertThat;

import javax.persistence.EntityManager;

import org.hibernate.ogm.utils.GridDialectType;
import org.hibernate.ogm.utils.SkipByGridDialect;
import org.hibernate.ogm.utils.jpa.JpaTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Nabeel Ali Memon &lt;nabeel@nabeelalimemon.com&gt;
 */
@SkipByGridDialect(value = { GridDialectType.HASHMAP }, comment = "Id generation strategy IDENTITY is not supported by the current grid dialect")
public class IdentityIdGeneratorTest extends JpaTestCase {

	private EntityManager em;

	@Before
	public void setUp() {
		em = getFactory().createEntityManager();
	}

	@After
	public void tearDown() {
		em.close();
	}

	@Test
	@SkipByGridDialect(value = GridDialectType.MONGODB, comment = "MongoDB supports IDENTITY columns, but not of type Long.")
	public void testIdentityGenerator() throws Exception {
		em.getTransaction().begin();
		Animal jungleKing = new Animal();
		Animal fish = new Animal();

		jungleKing.setName( "Lion" );
		jungleKing.setSpecies( "Mammal" );
		em.persist( jungleKing );

		fish.setName( "Shark" );
		fish.setSpecies( "Tiger Shark" );
		em.persist( fish );
		em.getTransaction().commit();
		em.clear();

		em.getTransaction().begin();
		Animal animal = em.find( Animal.class, jungleKing.getId() );
		assertThat( animal ).isNotNull();
		assertThat( animal.getId() ).isEqualTo( 1 );
		assertThat( animal.getName() ).isEqualTo( "Lion" );
		em.remove( animal );

		animal = em.find( Animal.class, fish.getId() );
		assertThat( animal ).isNotNull();
		assertThat( animal.getId() ).isEqualTo( 2 );
		assertThat( animal.getName() ).isEqualTo( "Shark" );
		em.remove( animal );
		em.getTransaction().commit();
	}

	@Override
	public Class<?>[] getEntities() {
		return new Class<?>[]{
			Animal.class
		};
	}
}
