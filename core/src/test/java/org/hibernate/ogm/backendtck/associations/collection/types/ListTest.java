/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.backendtck.associations.collection.types;

import static org.fest.assertions.Assertions.assertThat;

import org.hibernate.Session;
import org.hibernate.ogm.utils.OgmTestCase;
import org.junit.After;
import org.junit.Test;

/**
 * @author Emmanuel Bernard &lt;emmanuel@hibernate.org&gt;
 * @author Gunnar Morling
 */
public class ListTest extends OgmTestCase {

	private Father father;
	private Child luke, leia;
	private GrandMother grandMother;
	private Runner emmanuel, pere;
	private Race race;

	@After
	public void cleanUp() {
		inTransaction( ( session ) -> {
			delete( session, father, luke, leia );
			delete( session, grandMother );
			delete( session, race, emmanuel, pere );
		} );
		checkCleanCache();
	}

	private void delete(Session session, Object... entities) {
		for ( Object entity : entities ) {
			if ( entity != null ) {
				session.refresh( entity );
				session.delete( entity );
			}
		}
	}

	@Test
	public void testOrderedList() throws Exception {
		inTransaction( ( session ) -> {
			luke = new Child();
			luke.setName( "Luke" );
			session.persist( luke );

			leia = new Child();
			leia.setName( "Leia" );
			session.persist( leia );

			father = new Father();
			father.getOrderedChildren().add( luke );
			father.getOrderedChildren().add( null );
			father.getOrderedChildren().add( leia );
			session.persist( father );
		} );

		inTransaction( ( session ) -> {
			father = (Father) session.get( Father.class, father.getId() );
			assertThat( father.getOrderedChildren() )
					.as( "List should have 3 elements" )
					.hasSize( 3 );
			assertThat( father.getOrderedChildren().get( 0 ).getName() )
					.as( "Luke should be first" )
					.isEqualTo( "Luke" );
			assertThat( father.getOrderedChildren().get( 1 ) )
					.as( "Second born should be null" )
					.isNull();
			assertThat( father.getOrderedChildren().get( 2 ).getName() )
					.as( "Leia should be third" )
					.isEqualTo( "Leia" );
		} );
	}

	@Test
	public void testUpdateToElementOfOrderedListIsApplied() throws Exception {
		// insert entity with embedded collection
		inTransaction( ( session ) -> {
			GrandChild luke = new GrandChild();
			luke.setName( "Luke" );

			GrandChild leia = new GrandChild();
			leia.setName( "Leia" );

			grandMother = new GrandMother();
			grandMother.getGrandChildren().add( luke );
			grandMother.getGrandChildren().add( leia );
			session.persist( grandMother );
		} );

		// Update one of the elements
		inTransaction( ( session ) -> {
			grandMother = (GrandMother) session.get( GrandMother.class, grandMother.getId() );
			assertThat( grandMother.getGrandChildren() ).onProperty( "name" ).containsExactly( "Luke", "Leia" );
			grandMother.getGrandChildren().get( 0 ).setName( "Lisa" );
		} );

		// assert update has been propagated
		inTransaction( ( session ) -> {
			grandMother = (GrandMother) session.get( GrandMother.class, grandMother.getId() );
			assertThat( grandMother.getGrandChildren() ).onProperty( "name" ).containsExactly( "Lisa", "Leia" );
		} );
	}

	@Test
	public void testRemovalOfElementFromOrderedListIsApplied() throws Exception {
		// insert entity with embedded collection
		inTransaction( ( session ) -> {
			GrandChild luke = new GrandChild();
			luke.setName( "Luke" );

			GrandChild leia = new GrandChild();
			leia.setName( "Leia" );

			grandMother = new GrandMother();
			grandMother.getGrandChildren().add( luke );
			grandMother.getGrandChildren().add( leia );
			session.persist( grandMother );
		} );

		// remove one of the elements
		inTransaction( ( session ) -> {
			grandMother = (GrandMother) session.get( GrandMother.class, grandMother.getId() );
			grandMother.getGrandChildren().remove( 0 );
		} );

		// assert removal has been propagated
		inTransaction( ( session ) -> {
			grandMother = (GrandMother) session.get( GrandMother.class, grandMother.getId() );
			assertThat( grandMother.getGrandChildren() ).onProperty( "name" ).containsExactly( "Leia" );
		} );
	}

	@Test
	public void testOrderedListAndCompositeId() throws Exception {
		inTransaction( ( session ) -> {
			emmanuel = new Runner();
			emmanuel.setAge( 37 );
			emmanuel.setRunnerId( new Runner.RunnerId( "Emmanuel", "Bernard" ) );
			session.persist( emmanuel );

			pere = new Runner();
			pere.setAge( 105 );
			pere.setRunnerId( new Runner.RunnerId( "Pere", "Noel" ) );
			session.persist( pere );

			race = new Race();
			race.setRaceId( new Race.RaceId( 23, 75 ) );
			race.getRunnersByArrival().add( emmanuel );
			race.getRunnersByArrival().add( pere );
			session.persist( race );
		} );

		inTransaction( ( session ) -> {
			race = (Race) session.get( Race.class, race.getRaceId() );
			assertThat( race.getRunnersByArrival() ).containsExactly( emmanuel, pere );
		} );
	}

	@Override
	protected Class<?>[] getAnnotatedClasses() {
		return new Class<?>[] {
				Father.class,
				GrandMother.class,
				Child.class,
				Race.class,
				Runner.class
		};
	}
}
