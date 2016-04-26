/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.datastore.ogm.orientdb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.FlushModeType;
import javax.persistence.Persistence;
import javax.persistence.Query;

import org.hibernate.datastore.ogm.orientdb.jpa.Customer;
import org.hibernate.datastore.ogm.orientdb.jpa.Pizza;
import org.hibernate.datastore.ogm.orientdb.jpa.Status;
import org.hibernate.datastore.ogm.orientdb.utils.MemoryDBUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import com.orientechnologies.orient.core.id.ORecordId;
import org.apache.log4j.Logger;

/**
 * Test checks CRUD for simple entities (without links with other entities)
 *
 * @author Sergey Chernolyas (sergey.chernolyas@gmail.com)
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class OrientDBSimpleTest {

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
	public void test1InsertNewCustomer() {
		log.debug( "start" );
		try {
			em.getTransaction().begin();
			Customer newCustomer = new Customer();
			newCustomer.setName( "test" );
			newCustomer.setStatus( Status.VIP );
			log.debug( "New Customer ready for  persit" );
			em.persist( newCustomer );
			em.flush();
			Query query = em.createNativeQuery( "select from Customer where name=:name", Customer.class );
			query.setParameter( "name", "test" );
			List<Customer> customers = query.getResultList();
			log.debug( String.format( "customers.size(): %s", customers.size() ) );
			assertFalse( "Customers must be", customers.isEmpty() );
			Customer testCustomer = customers.get( 0 );
			assertNotNull( "Customer with 'test' must be saved!", testCustomer );
			em.getTransaction().commit();
		}
		catch (Exception e) {
			log.error( "Error", e );
			em.getTransaction().rollback();
			throw e;
		}
	}

	@Test
	public void test1InsertNewPizza() throws Exception {
		log.debug( "start" );
		try {
			em.getTransaction().begin();
			Pizza newPizza = new Pizza();
			newPizza.setName( "Marinero" );
			em.persist( newPizza );
			em.flush();
			Query query = em.createNativeQuery( "select from Pizza where name=:name", Pizza.class );
			query.setParameter( "name", "Marinero" );
			List<Pizza> pizzaList = query.getResultList();
			assertFalse( "pizzaList must be not empty!", pizzaList.isEmpty() );
			Pizza testPizza = pizzaList.get( 0 );
			assertNotNull( "Pizza with 'Marinero' must be saved!", testPizza );
			em.getTransaction().commit();
		}
		catch (Exception e) {
			log.error( "Error", e );
			em.getTransaction().rollback();
			throw e;
		}
	}

	@Test
	public void test2FindCustomer() {
		log.debug( "start" );
		try {
			em.getTransaction().begin();
			Customer customer = em.find( Customer.class, 1L );
			em.refresh( customer );
			log.debug( "read entity properties:" );
			log.debug( "customer.getbKey():" + customer.getbKey() );
			log.debug( "customer.getName(): " + customer.getName() );
			log.debug( "customer.getRid(): " + customer.getRid() );
			log.debug( "customer.isBlocked(): " + customer.isBlocked() );
			log.debug( "customer.getCreatedDate(): " + customer.getCreatedDate() );
			assertEquals( Long.valueOf( 1L ), customer.getbKey() );
			assertNotNull( customer.getRid() );
		}
		finally {
			em.getTransaction().commit();
		}
	}

	@Test
	public void test2FindPizza() {
		log.debug( "start" );
		try {
			em.getTransaction().begin();
			Pizza pizza = em.find( Pizza.class, 1L );
			em.refresh( pizza );
			log.debug( "read entity properties:" );
			log.debug( "pizza.getBKey():" + pizza.getbKey() );
			log.debug( "pizza.getName(): " + pizza.getName() );
			assertEquals( Long.valueOf( 1L ), pizza.getbKey() );
			assertNotNull( pizza.getRid() );
		}
		finally {
			em.getTransaction().commit();
		}

	}

	@Test
	public void test3CreateNativeQuery() {
		log.debug( "start" );
		try {
			em.getTransaction().begin();
			log.debug( "query: select from Customer" );
			Query query = em.createNativeQuery( "select from Customer", Customer.class );
			List<Customer> customers = query.getResultList();
			assertFalse( "Customers must be", customers.isEmpty() );
			assertEquals( Long.valueOf( 1L ), customers.get( 0 ).getbKey() );

			log.debug( "query: select from " + customers.get( 0 ).getRid().toString() );
			query = em.createNativeQuery( "select from " + customers.get( 0 ).getRid().toString(), Customer.class );
			customers = query.getResultList();
			assertFalse( "Customers must be", customers.isEmpty() );

			log.debug( "query: select from Customer where name=:name" );
			query = em.createNativeQuery( "select from Customer where name=:name", Customer.class );
			query.setParameter( "name", "test" );
			customers = query.getResultList();
			assertFalse( "Customers must be", customers.isEmpty() );

			log.debug( "query: select from Customer where name='test'" );
			query = em.createNativeQuery( "select from Customer where name='test'", Customer.class );
			customers = query.getResultList();
			assertFalse( "Customers must be", customers.isEmpty() );

		}
		finally {
			em.getTransaction().commit();
		}

	}

	@Test
	public void test4UpdateCustomer() {
		log.debug( "start" );
		try {
			em.getTransaction().begin();
			Long id = 1L;
			Customer customer = em.find( Customer.class, id );
			customer.setName( "Ivahoe" );
			int oldVersion = customer.getVersion();
			log.debug( "old version:" + oldVersion );
			em.merge( customer );
			em.flush();
			Customer newCustomer = em.find( Customer.class, id );
			assertNotNull( "Must not be null", newCustomer );
			assertEquals( customer.getRid(), newCustomer.getRid() );
			assertEquals( "Ivahoe", newCustomer.getName() );
			int newVersion = newCustomer.getVersion();
			log.debug( "new version:" + newVersion );
			assertTrue( "Version must be chanched", ( newVersion > oldVersion ) );
			em.getTransaction().commit();
		}
		catch (Exception e) {
			log.error( "Error", e );
			em.getTransaction().rollback();
			throw e;
		}
	}

	@Test
	public void test5RefreshCustomer() {
		log.debug( "start" );
		try {
			em.getTransaction().begin();
			Long id = 1L;
			Customer customer = em.find( Customer.class, id );
			log.debug( "old rid:{0}" + customer.getRid() );
			ORecordId oldRid = customer.getRid();
			em.refresh( customer );
			assertNotNull( "Must not be null", customer );
			ORecordId newRid = customer.getRid();

			if ( oldRid == null ) {
				assertNotNull( "@Rid must be changed", newRid );
			}
			else {
				assertEquals( "@Rid must not changed", newRid, oldRid );
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
	public void test6RemoveCustomer() {
		log.debug( "start" );
		try {
			em.getTransaction().begin();
			Long id = 1L;
			Customer customer = em.find( Customer.class, id );
			em.remove( customer );
			em.flush();
			em.getTransaction().commit();

			em.getTransaction().begin();
			Customer removedCustomer = em.find( Customer.class, id );
			assertNull( "removedCustomer must be null!", removedCustomer );
			em.getTransaction().commit();

		}
		catch (Exception e) {
			log.error( "Error", e );
			em.getTransaction().rollback();
			throw e;
		}
	}

}
