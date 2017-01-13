/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.mongodb.test.embeddable;

import static org.fest.assertions.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.CollectionTable;
import javax.persistence.ElementCollection;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

import org.hibernate.Transaction;
import org.hibernate.ogm.OgmSession;
import org.hibernate.ogm.utils.OgmTestCase;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Davide D'Alto
 */
public class ElementCollectionOrderTest extends OgmTestCase {

	private static final String ENTITY_ID = "Entity";
	private static final String[] EVENTS = {
			"event 5",
			"event 3",
			"event 1",
			"event 2",
			"event 4",
			// "event 1", This does not work at the moment
			"event 14",
			"event x"
	};

	@Before
	public void before() {
		try ( OgmSession session = openSession() ) {
			Transaction tx = session.beginTransaction();
			PlainEntity entity = new PlainEntity();
			entity.id = ENTITY_ID;
			for ( String event : EVENTS ) {
				entity.events.add( event );
			}
			session.persist( entity );
			tx.commit();
		}
	}

	@Test
	public void testOrderIsMaintained() {
		try ( OgmSession session = openSession() ) {
			Transaction tx = session.beginTransaction();
			PlainEntity entity = session.get( PlainEntity.class, ENTITY_ID );

			assertThat( entity ).isNotNull();
			assertThat( entity.id ).isEqualTo( ENTITY_ID );
			for ( int i = 0; i < EVENTS.length; i++ ) {
				assertThat( entity.events.get( i ) ).isEqualTo( EVENTS[i] );
			}
			tx.commit();
		}
	}

	@Override
	protected Class<?>[] getAnnotatedClasses() {
		return new Class[]{ PlainEntity.class };
	}

	@Entity
	@Table(name = "PlainEntity")
	static class PlainEntity {

		@Id
		String id;

		@ElementCollection
		@CollectionTable(name = "events")
		List<String> events = new ArrayList<>();
	}
}
