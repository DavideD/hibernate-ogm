/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.datastore.ogm.orientdb;

import com.orientechnologies.orient.core.id.ORecordId;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityNotFoundException;
import javax.persistence.FlushModeType;
import javax.persistence.Persistence;
import javax.persistence.Query;
import javax.persistence.TypedQuery;
import org.apache.log4j.BasicConfigurator;
import org.hibernate.CacheMode;
import org.hibernate.datastore.ogm.orientdb.jpa.Customer;
import org.hibernate.datastore.ogm.orientdb.jpa.Pizza;
import org.hibernate.datastore.ogm.orientdb.util.MemoryDBUtil;
import org.hibernate.search.MassIndexer;
import org.hibernate.search.jpa.FullTextEntityManager;
import org.hibernate.search.jpa.FullTextQuery;
import org.hibernate.search.jpa.Search;
import org.hibernate.search.query.DatabaseRetrievalMethod;
import org.hibernate.search.query.ObjectLookupMethod;
import org.hibernate.search.query.dsl.QueryBuilder;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.FixMethodOrder;
import org.junit.runners.MethodSorters;

/**
 * @author Sergey Chernolyas (sergey.chernolyas@gmail.com)
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class OrientDBTest {

	private static final Logger LOG = Logger.getLogger( OrientDBTest.class.getName() );
	private static Map<String, List<ORecordId>> classRidMap;
	private static EntityManager em;
	private static EntityManagerFactory emf;

	public OrientDBTest() {
	}

	@BeforeClass
	public static void setUpClass() {
		LOG.log( Level.INFO, "start" );
		//classRidMap = MemoryDBUtil.prepareDb( "memory:test" );
                classRidMap = new HashMap<>();
		// classIdMap = MemoryDBUtil.prepareDb("remote:localhost/pizza");                
                MemoryDBUtil.createInMemoryDB("memory:test");
		BasicConfigurator.configure();
		emf = Persistence.createEntityManagerFactory( "hibernateOgmJpaUnit" );
		em = emf.createEntityManager();
		em.setFlushMode( FlushModeType.COMMIT );

	}

	@AfterClass
	public static void tearDownClass() {
		LOG.log( Level.INFO, "start" );
		if ( em != null ) {
			em.close();
			emf.close();
		}
	}

	@Before
	public void setUp() {
	}

	@After
	public void tearDown() {
	}
        
        @Test
	public void test1InsertNewCustomer() throws Exception {
		LOG.log( Level.INFO, "start" );
		try {
			em.getTransaction().begin();
			Customer newCustomer = new Customer();
			newCustomer.setName( "test" );
			LOG.log( Level.INFO, "New Customer ready for  persit" );
			em.persist( newCustomer );
			em.flush();
			Query query = em.createNativeQuery( "select from Customer where name=:name", Customer.class );
			query.setParameter( "name", "test" );
			List<Customer> customers = query.getResultList();
			LOG.log( Level.INFO, "customers.size(): {0}" , customers.size() );
			assertFalse( "Customers must be", customers.isEmpty() );
			Customer testCustomer = customers.get( 0 );
			assertNotNull( "Customer with 'test' must be saved!", testCustomer );
			em.getTransaction().commit();
		}
		catch (Exception e) {
			LOG.log( Level.SEVERE, "Error", e );
			em.getTransaction().rollback();
			throw e;
		}
	}

	@Test
	public void test1InsertNewPizza() throws Exception {
		LOG.log( Level.INFO, "start" );
		try {
			em.getTransaction().begin();
			Pizza newPizza = new Pizza();
			newPizza.setName( "Marinero" );
			LOG.log( Level.INFO, "New Pizza ready for  persit" );
			em.persist( newPizza );
			em.flush();
			Query query = em.createNativeQuery( "select from Pizza where name=:name", Pizza.class );
			query.setParameter( "name", "Marinero" );
			List<Pizza> pizzaList = query.getResultList();
			LOG.log( Level.INFO, "pizzaList.size(): {0}", pizzaList.size() );
			assertFalse( "pizzaList must be not empty!", pizzaList.isEmpty() );
			Pizza testPizza = pizzaList.get( 0 );
			assertNotNull( "Pizza with 'Marinero' must be saved!", testPizza );
			em.getTransaction().commit();
		}
		catch (Exception e) {
			LOG.log( Level.SEVERE, "Error", e );
			em.getTransaction().rollback();
			throw e;
		}
	}
        
        
        @Test
	public void test2FindCustomer() {
		LOG.log( Level.INFO, "start" );
		try {
			em.getTransaction().begin();
			Customer customer = em.find( Customer.class, Long.valueOf( 2L ) );
                        em.refresh(customer);
			LOG.log( Level.INFO, "read entity properties:" );
			LOG.log( Level.INFO, "customer.getbKey():{0}", customer.getbKey() );
			LOG.log( Level.INFO, "customer.getName(): {0}", customer.getName() );			
                        LOG.log( Level.INFO, "customer.getRid(): {0}", customer.getRid() );
                        assertEquals(Long.valueOf(2L),customer.getbKey() );
                        assertNotNull( customer.getRid() );                        
		}
		finally {
			em.getTransaction().commit();
		}
	}
        
        //@Test
	public void test2FindCustomerWithAssociation() {
		LOG.log( Level.INFO, "start" );
		try {
			em.getTransaction().begin();
			Customer customer = em.find( Customer.class, Long.valueOf( 2L ) );
                        em.refresh(customer);
			LOG.log( Level.INFO, "read entity properties:" );
			LOG.log( Level.INFO, "customer.getbKey():{0}", customer.getbKey() );
			LOG.log( Level.INFO, "customer.getName(): {0}", customer.getName() );			
                        LOG.log( Level.INFO, "customer.getRid(): {0}", customer.getRid() );
                        assertEquals(Long.valueOf(2L),customer.getbKey() );
                        assertNotNull( customer.getRid() );
                        customer.getOrders().size();
		}
		finally {
			em.getTransaction().commit();
		}
	
        }

	@Test
	public void test2FindPizza() {
		LOG.log( Level.INFO, "start" );
		try {
			em.getTransaction().begin();
			Pizza pizza = em.find( Pizza.class, Long.valueOf( 2L ) );
                        em.refresh(pizza);
			LOG.log( Level.INFO, "read entity properties:" );
			LOG.log( Level.INFO, "pizza.getBKey():{0}", pizza.getbKey() );
			LOG.log( Level.INFO, "pizza.getName(): {0}", pizza.getName() );
                        LOG.log( Level.INFO, "pizza.getbKey(): {0}", pizza.getbKey() );
			assertEquals( Long.valueOf( 2L ), pizza.getbKey() );
                        assertNotNull( pizza.getRid() );
		}
		finally {
			em.getTransaction().commit();
		}

	}

	@Test
	public void test3CreateNativeQuery() {
		LOG.log( Level.INFO, "start" );
		try {
			em.getTransaction().begin();
			LOG.log( Level.INFO, "query: select from Customer" );
			Query query = em.createNativeQuery( "select from Customer", Customer.class );
			List<Customer> customers = query.getResultList();
			assertFalse( "Customers must be", customers.isEmpty() );			
			assertEquals( Long.valueOf( 2L ), customers.get( 0 ).getbKey() );

			LOG.log( Level.INFO, "query: select from {0}", customers.get( 0 ).getRid().toString() );
			query = em.createNativeQuery( "select from " + customers.get( 0 ).getRid().toString(), Customer.class );
			customers = query.getResultList();
			assertFalse( "Customers must be", customers.isEmpty() );

			LOG.log( Level.INFO, "query: select from Customer where name=:name" );
			query = em.createNativeQuery( "select from Customer where name=:name", Customer.class );
			query.setParameter( "name", "test" );
			customers = query.getResultList();
			assertFalse( "Customers must be", customers.isEmpty() );

			LOG.log( Level.INFO, "query: select from Customer where name='test'" );
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
		LOG.log( Level.INFO, "start" );
		try {
			em.getTransaction().begin();
			Long id = Long.valueOf(2L);
			Customer customer = em.find( Customer.class, id );
			customer.setName( "Ivahoe" );
			int oldVersion = customer.getVersion();
			LOG.log( Level.INFO, "old version:{0}", oldVersion );
			em.merge( customer );
			em.flush();
			Customer newCustomer = em.find( Customer.class, id );
			assertNotNull( "Must not be null", newCustomer );
			assertEquals( customer.getRid(), newCustomer.getRid() );
			assertEquals( "Ivahoe", newCustomer.getName() );
			int newVersion = newCustomer.getVersion();
			LOG.log( Level.INFO, "new version:{0}", newVersion );
			assertTrue( "Version must be chanched", ( newVersion > oldVersion ) );
			em.getTransaction().commit();
		}
		catch (Exception e) {
			LOG.log( Level.SEVERE, "Error", e );
			em.getTransaction().rollback();
			throw e;
		}
	}
        
        
        @Test
	public void test5RefreshCustomer() {
		LOG.log( Level.INFO, "start" );
		try {
			em.getTransaction().begin();
			Long id = Long.valueOf(2L);
			Customer customer = em.find( Customer.class, id );
			LOG.log( Level.INFO, "old rid:{0}", customer.getRid() );
                        ORecordId oldRid = customer.getRid();
			em.refresh(customer );			
                        ORecordId newRid = customer.getRid();
			assertNotNull( "Must not be null", customer );                        
                        if (oldRid==null) {
                            assertNotNull( "@Rid must be changed", newRid );                        
                        } else {
                            assertEquals("@Rid must not changed", newRid,oldRid );                        
                        }
			em.getTransaction().commit();
		}
		catch (Exception e) {
			LOG.log( Level.SEVERE, "Error", e );
			em.getTransaction().rollback();
			throw e;
		}
	}

	 @Test
	public void test6RemoveCustomer() {
		LOG.log( Level.INFO, "start" );
		try {
			em.getTransaction().begin();
			Long id = Long.valueOf(2L);
			Customer customer = em.find( Customer.class, id );
			em.remove( customer );
			em.flush();
                        em.getTransaction().commit();
                        
                        em.getTransaction().begin();
			Customer removedCustomer = em.find( Customer.class, id );
			LOG.log( Level.INFO, "customer:{0}", removedCustomer );
                        assertNull("removedCustomer must be null!", removedCustomer);
                        em.getTransaction().commit();
			
		}
		catch (Exception e) {
			LOG.log( Level.SEVERE, "Error", e );
			em.getTransaction().rollback();
			throw e;
		}
	}
        
        
        

	// @Test yet not work
	public void createNamedQuery() throws Exception {
		System.out.println( "org.hibernate.datastore.ogm.orientdb.OrientDBTestIT.createNamedQuery()" );
		try {
			em.getTransaction().begin();
			FullTextEntityManager fullTextEntityManager = Search.getFullTextEntityManager( em );
			MassIndexer indexer = fullTextEntityManager.createIndexer();
			indexer.cacheMode( CacheMode.REFRESH );
			indexer.startAndWait();

			System.out.println( "entities has indexed" );

			System.out.println( "named query: Customer.findAll" );
			TypedQuery<Customer> query = em.createNamedQuery( "Customer.findAll", Customer.class );
			List<Customer> customers = query.getResultList();
			System.out.println( "1.customers.size(): " + customers.size() );

			QueryBuilder qb = fullTextEntityManager.getSearchFactory()
					.buildQueryBuilder().forEntity( Customer.class ).get();

			org.apache.lucene.search.Query luceneQuery = qb.all().createQuery();
			FullTextQuery jpaQuery = fullTextEntityManager.createFullTextQuery( luceneQuery, Customer.class );
			jpaQuery.initializeObjectsWith( ObjectLookupMethod.PERSISTENCE_CONTEXT, DatabaseRetrievalMethod.QUERY );

			customers = jpaQuery.getResultList();
			System.out.println( "2.customers.size(): " + customers.size() );

			assertFalse( "Customers must be", customers.isEmpty() );
			ORecordId rid = classRidMap.get( "Customer" ).get( 0 );
			assertEquals( rid, customers.get( 0 ).getRid() );

		}
		finally {
			em.getTransaction().commit();
		}

	}

}
