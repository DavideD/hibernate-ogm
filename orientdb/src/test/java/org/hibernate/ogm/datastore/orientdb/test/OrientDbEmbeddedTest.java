/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.orientdb.test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.LinkedList;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.FlushModeType;
import javax.persistence.Persistence;

import org.apache.log4j.Logger;
import org.hibernate.ogm.datastore.orientdb.test.jpa.Car;
import org.hibernate.ogm.datastore.orientdb.test.jpa.CarOwner;
import org.hibernate.ogm.datastore.orientdb.test.jpa.EngineInfo;
import org.hibernate.ogm.datastore.orientdb.test.jpa.Producer;
import org.hibernate.ogm.datastore.orientdb.utils.MemoryDBUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/**
 * @author Sergey Chernolyas &lt;sergey.chernolyas@gmail.com&gt;
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class OrientDbEmbeddedTest {

	public static final String MEMORY_TEST = "memory:test";
	private static final Logger log = Logger.getLogger( OrientDBSimpleTest.class.getName() );
	private static EntityManager em;
	private static EntityManagerFactory emf;

	@BeforeClass
	public static void setUpClass() {
		MemoryDBUtil.createDbFactory( MEMORY_TEST );
		emf = Persistence.createEntityManagerFactory( "hibernateOgmJpaUnit" );
		em = emf.createEntityManager();
		em.setFlushMode( FlushModeType.COMMIT );
	}

	@AfterClass
	public static void tearDownClass() {
		if ( em != null ) {
			em.close();
			emf.close();

		}
		MemoryDBUtil.dropInMemoryDb();
	}

	@Before
	public void setUp() {
		if ( em.getTransaction().isActive() ) {
			em.getTransaction().rollback();
		}
	}

	@After
	public void tearDown() {
		em.clear();
	}

	@Test
	public void test1InsertNewEmbeddedClass() {
		log.debug( "insertNewEmbeddedClass" );
		try {
			em.getTransaction().begin();
			Producer producer = new Producer();
			producer.setCountry( "RU" );
			producer.setTitle( "Rostech" );
			producer.setPhone( "+74956667788" );

			Car newCar = new Car();
			EngineInfo engineInfo = new EngineInfo();
			engineInfo.setTitle( "E6GV" );
			engineInfo.setCylinders( (short) 6 );
			engineInfo.setPower( 100 );
			engineInfo.setPrice( 1000000 );
			engineInfo.setProducer( producer );
			newCar.setEngineInfo( engineInfo );

			List<CarOwner> owners = new LinkedList<>();
			owners.add( new CarOwner( "name1", true ) );
			owners.add( new CarOwner( "name2", false ) );
			newCar.setOwners( owners );
			newCar.setPhoto( new byte[]{ 1, 2, 3 } );

			em.persist( newCar );
			em.getTransaction().commit();
			em.clear();
			// reread
			em.getTransaction().begin();
			Car car = em.find( Car.class, 1l );
			assertNotNull( "Car must be saved!", car );
			assertArrayEquals( "Photo of car  must be saved correctly!", new byte[]{ 1, 2, 3 }, car.getPhoto() );
			assertNotNull( "Engine info of saved Car must be saved!", car.getEngineInfo() );
			assertEquals( "E6GV", car.getEngineInfo().getTitle() );
			assertNotNull( "Producer of Engine info of saved Car must be saved!", car.getEngineInfo().getProducer() );
			assertEquals( "+74956667788", car.getEngineInfo().getProducer().getPhone() );
			assertNotNull( "Car must have owners!", car.getOwners() );
			assertEquals( "Car must have 2 owners!", 2, car.getOwners().size() );
			for ( CarOwner owner : owners ) {
				assertTrue( "Unkwoun owner :" + owner.getName(),
						( owner.getName().equals( "name1" ) || owner.getName().equals( "name2" ) ) );
				if ( owner.getName().equals( "name1" ) ) {
					assertTrue( "Owner with name1 must be active", owner.isActive() );
				}
				else if ( owner.getName().equals( "name2" ) ) {
					assertFalse( "Owner with name2 must be not active", owner.isActive() );
				}
			}

			em.getTransaction().commit();
		}
		catch (Exception e) {
			log.error( "Error", e );
			em.getTransaction().rollback();
			throw e;
		}
	}

	@Test
	public void test2UpdateEmbeddedClass() {
		log.debug( "updateEmbeddedClass" );
		try {
			em.getTransaction().begin();
			Car car = em.find( Car.class, 1l );
			assertNotNull( "Car must be saved!", car );
			car.setPhoto( new byte[]{ 3, 2, 1 } );
			em.merge( car );
			em.getTransaction().commit();

			em.clear();

			em.getTransaction().begin();
			car = em.find( Car.class, 1l );
			assertNotNull( "Car must be updated!", car );
			assertArrayEquals( "Photo of car  must be updated correctly!", new byte[]{ 3, 2, 1 }, car.getPhoto() );
			assertEquals( "Owners of car must not be changed!", car.getOwners().size(), 2L );
			em.getTransaction().commit();
		}
		catch (Exception e) {
			log.error( "Error", e );
			em.getTransaction().rollback();
			throw e;
		}
	}

	@Test
	public void test3UpdateEmbeddedCollection() {
		log.debug( "updateEmbeddedCollection" );
		try {
			em.getTransaction().begin();
			Car car = em.find( Car.class, 1l );
			assertNotNull( "Car must be saved!", car );

			List<CarOwner> owners = car.getOwners();
			for ( CarOwner owner : owners ) {
				owner.setActive( false );
			}
			em.merge( car );
			owners.add( new CarOwner( "name3", true ) );
			em.merge( car );
			em.getTransaction().commit();

			em.clear();

			em.getTransaction().begin();
			car = em.find( Car.class, 1l );
			assertNotNull( "Car must be updated!", car );
			assertEquals( "Count of owners must be changed!", car.getOwners().size(), 3L );
			em.getTransaction().commit();
		}
		catch (Exception e) {
			log.error( "Error", e );
			em.getTransaction().rollback();
			throw e;
		}
	}

	@Test
	public void test4RemoveEmbeddedCollection() {
		log.debug( "removeEmbeddedCollection" );
		try {
			em.getTransaction().begin();
			Car car = em.find( Car.class, 1l );
			assertNotNull( "Car must be saved!", car );

			List<CarOwner> owners = car.getOwners();
			List<CarOwner> newOwners = new LinkedList<>();
			for ( CarOwner owner : owners ) {
				if ( !owner.getName().equals( "name3" ) ) {
					newOwners.add( owner );
				}
			}
			car.setOwners( newOwners );
			em.merge( car );
			em.getTransaction().commit();
			em.clear();

			em.getTransaction().begin();
			car = em.find( Car.class, 1l );
			assertNotNull( "Car must be updated!", car );
			assertEquals( "Count of owners must be changed!", car.getOwners().size(), 2L );
			em.getTransaction().commit();
		}
		catch (Exception e) {
			log.error( "Error", e );
			em.getTransaction().rollback();
			throw e;
		}
	}

}
