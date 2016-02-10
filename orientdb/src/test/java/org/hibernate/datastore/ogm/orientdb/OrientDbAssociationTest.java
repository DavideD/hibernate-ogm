/*
* Hibernate OGM, Domain model persistence for NoSQL datastores
* 
* License: GNU Lesser General Public License (LGPL), version 2.1 or later
* See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
*/

package org.hibernate.datastore.ogm.orientdb;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.FlushModeType;
import javax.persistence.Persistence;
import javax.persistence.Query;
import org.apache.log4j.BasicConfigurator;
import org.hibernate.datastore.ogm.orientdb.jpa.BuyingOrder;
import org.hibernate.datastore.ogm.orientdb.jpa.Customer;
import org.hibernate.datastore.ogm.orientdb.util.MemoryDBUtil;
import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/**
 * Test checks CRUD for entities with associations (with links with other entities)
 * 
 * @author Sergey Chernolyas <sergey.chernolyas@gmail.com>
 */

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class OrientDbAssociationTest {

	private static final Logger LOG = Logger.getLogger( OrientDbAssociationTest.class.getName() );
	private static EntityManager em;
	private static EntityManagerFactory emf;

	@BeforeClass
	public static void setUpClass() {
		LOG.log( Level.INFO, "start" );
		// MemoryDBUtil.prepareDb("remote:localhost/pizza");
		MemoryDBUtil.createDbFactory( "memory:test" );
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
		MemoryDBUtil.getOrientGraphFactory().close();
	}

	@Before
	public void setUp() {
	}

	@After
	public void tearDown() {
		em.clear();
	}

	@Test
	public void test1InsertNewCustomerWithAssociation() throws Exception {
		LOG.log( Level.INFO, "start" );
		// @TODO check saving links!!!
		try {
			em.getTransaction().begin();
			Customer newCustomer = new Customer();
			newCustomer.setName( "Ivahoe" );
			em.persist( newCustomer );

			BuyingOrder buyingOrder = new BuyingOrder();
			buyingOrder.setOrderKey( "2233" );
			em.persist( buyingOrder );
			em.flush();
			em.getTransaction().commit();

			em.getTransaction().begin();
			Query query = em.createNativeQuery( "select from Customer where name='Ivahoe'", Customer.class );
			List<Customer> customers = query.getResultList();
			LOG.log( Level.INFO, "customers.size(): {0}", customers.size() );
			assertFalse( "Customers must be", customers.isEmpty() );
			Customer customer = customers.get( 0 );
			em.refresh( customer );// @TODO ... Think!
			assertNotNull( "Customer with 'Ivahoe' must be saved!", customer );

			query = em.createNativeQuery( "select from BuyingOrder where orderKey='2233'", BuyingOrder.class );
			List<BuyingOrder> orders = query.getResultList();
			LOG.log( Level.INFO, "orders.size(): {0}", orders.size() );
			buyingOrder = orders.get( 0 );
			em.refresh( buyingOrder );

			LOG.log( Level.INFO, "create link between  {0} and {1}", new Object[]{ customer, buyingOrder } );

			buyingOrder.setOwner( customer );
			em.merge( buyingOrder );

			List<BuyingOrder> linkedOrders = new LinkedList<>();
			linkedOrders.add( buyingOrder );
			customer.setOrders( linkedOrders );
			em.merge( customer );

			em.getTransaction().commit();
		}
		catch (Exception e) {
			LOG.log( Level.SEVERE, "Error", e );
			em.getTransaction().rollback();
			throw e;
		}
	}

	@Test
	public void test2FindCustomerWithAssociation() throws Exception {
		LOG.log( Level.INFO, "start" );
		try {
			em.getTransaction().begin();
			Query query = em.createNativeQuery( "select from Customer where name='Ivahoe'", Customer.class );
			List<Customer> customers = query.getResultList();
			LOG.log( Level.INFO, "customers.size(): {0}", customers.size() );
			assertFalse( "Customers must be", customers.isEmpty() );
			Customer customer = customers.get( 0 );
			LOG.log( Level.INFO, "use Customer with id {0} ( rid: {1} )", new Object[]{ customer.getbKey(), customer.getRid() } );
			assertNotNull( "Customer with 'Ivahoe' must be saved!", customer );
			assertTrue( "Customer must to have orders!", customer.getOrders().size() > 0 );
			Set<String> orderKeySet = new HashSet<>();
			LOG.log( Level.INFO, "orders :{0}", customer.getOrders().size() );
			for ( BuyingOrder order : customer.getOrders() ) {
				LOG.log( Level.INFO, "order.orderKey:{0}; id: {1}",
						new Object[]{ order.getOrderKey(), order.getbKey() } );
				orderKeySet.add( order.getOrderKey() );
			}
			LOG.log( Level.INFO, "OrderKeys : {0}", orderKeySet );
			assertTrue( "OrderKey 2233 must be linked!", orderKeySet.contains( "2233" ) );

			BuyingOrder order = customer.getOrders().get( 0 );
			assertNotNull( "Order with id '" + order.getbKey() + "' must to have owner!", order.getOwner() );

			em.getTransaction().commit();
		}
		catch (Exception e) {
			LOG.log( Level.SEVERE, "Error", e );
			em.getTransaction().rollback();
			throw e;
		}
	}

}