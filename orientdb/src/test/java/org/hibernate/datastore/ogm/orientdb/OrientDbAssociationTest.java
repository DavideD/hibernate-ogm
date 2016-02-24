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
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import org.apache.log4j.Logger;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.FlushModeType;
import javax.persistence.Persistence;
import javax.persistence.Query;

import org.apache.log4j.BasicConfigurator;
import org.hibernate.datastore.ogm.orientdb.jpa.BuyingOrder;
import org.hibernate.datastore.ogm.orientdb.jpa.Customer;
import org.hibernate.datastore.ogm.orientdb.jpa.Pizza;
import org.hibernate.datastore.ogm.orientdb.jpa.Product;
import org.hibernate.datastore.ogm.orientdb.jpa.ProductType;
import org.hibernate.datastore.ogm.orientdb.utils.MemoryDBUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import java.text.MessageFormat;

/**
 * Test checks CRUD for entities with associations (with links with other entities)
 *
 * @author Sergey Chernolyas <sergey.chernolyas@gmail.com>
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class OrientDbAssociationTest {

	private static final Logger log = Logger.getLogger( OrientDbAssociationTest.class.getName() );
	private static EntityManager em;
	private static EntityManagerFactory emf;
	private static OrientGraphNoTx graphNoTx;

	@BeforeClass
	public static void setUpClass() {
		graphNoTx = MemoryDBUtil.createDbFactory( OrientDBSimpleTest.MEMORY_TEST );
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
		graphNoTx.shutdown();
		MemoryDBUtil.recrateInMemoryDn( OrientDBSimpleTest.MEMORY_TEST );

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
	public void test1LinkAllAssociations() throws Exception {
		log.debug(  "start" );

		try {
			em.getTransaction().begin();

			// PhoneNumber phoneNumber = new PhoneNumber(new PhoneNumber.PhoneNumberId("+7", 4957778899L), "home");
			// em.persist(phoneNumber);

			Customer customer = new Customer();
			customer.setName( "Ivahoe" );
			em.persist( customer );

			// customer.setPhones(new LinkedList<PhoneNumber>(Arrays.asList(phoneNumber)));
			// em.merge(customer);

			BuyingOrder buyingOrder1 = new BuyingOrder();
			buyingOrder1.setOrderKey( "2233" );
			em.persist( buyingOrder1 );

			BuyingOrder buyingOrder2 = new BuyingOrder();
			buyingOrder2.setOrderKey( "3322" );
			em.persist( buyingOrder2 );

			ProductType type1 = new ProductType();
			type1.setDescription( "vegetables" );
			em.persist( type1 );

			ProductType type2 = new ProductType();
			type2.setDescription( "fruits" );
			em.persist( type2 );

			Product sausage = new Product();
			sausage.setName( "Sausage" );
			em.persist( sausage );

			Product olive = new Product();
			olive.setName( "Olive" );
			em.persist( olive );

			Product cheese = new Product();
			cheese.setName( "Cheese" );
			em.persist( cheese );

			Pizza pizza1 = new Pizza();
			pizza1.setName( "Super Papa" );
			em.persist( pizza1 );

			Pizza pizza2 = new Pizza();
			pizza2.setName( "Cheese" );
			em.persist( pizza2 );

			pizza2.setProducts( new LinkedList<>( Arrays.asList( cheese, olive ) ) );
			em.merge( pizza2 );

			pizza1.setProducts( new LinkedList<>( Arrays.asList( cheese, olive, sausage ) ) );
			em.merge( pizza1 );

			cheese.setPizzas( new LinkedList<Pizza>( Arrays.asList( pizza1, pizza2 ) ) );
			em.merge( cheese );
			olive.setPizzas( new LinkedList<Pizza>( Arrays.asList( pizza1, pizza2 ) ) );
			em.merge( olive );
			sausage.setPizzas( new LinkedList<Pizza>( Arrays.asList( pizza1 ) ) );

			buyingOrder1.setOwner( customer );
			em.merge( buyingOrder1 );

			buyingOrder2.setOwner( customer );
			em.merge( buyingOrder2 );

			List<BuyingOrder> linkedOrders = new LinkedList<>();
			linkedOrders.addAll( Arrays.asList( buyingOrder1, buyingOrder2 ) );
			customer.setOrders( linkedOrders );
			em.merge( customer );

			em.getTransaction().commit();
		}
		catch (Exception e) {
			log.error( "Error", e );
			em.getTransaction().rollback();
			throw e;
		}
	}

	@Test
	public void test2AddNewAssociations() throws Exception {
		log.debug(  "start" );
		try {
			em.getTransaction().begin();
			BuyingOrder buyingOrder3 = new BuyingOrder();
			buyingOrder3.setOrderKey( "4433" );
			em.persist( buyingOrder3 );
			Query query = em.createNativeQuery( "select from Customer where name='Ivahoe'", Customer.class );
			Customer customer = (Customer) query.getResultList().get( 0 );

			buyingOrder3.setOwner( customer );
			em.merge( buyingOrder3 );

			List<BuyingOrder> list = customer.getOrders();
			list.add( buyingOrder3 );
			em.merge( customer );
			em.getTransaction().commit();

			em.clear();

			em.getTransaction().begin();

			query = em.createNativeQuery( "select from Customer where name='Ivahoe'", Customer.class );
			customer = (Customer) query.getResultList().get( 0 );
			list = customer.getOrders();
			assertEquals( 3l, list.size() );
			em.getTransaction().commit();

		}
		catch (Exception e) {
			log.error(  "Error", e );
			em.getTransaction().rollback();
			throw e;
		}
	}

	@Test
	public void test3RemoveAssociations() throws Exception {
		log.debug( "start" );
		try {
			em.getTransaction().begin();
			List<BuyingOrder> list = null;

			Customer customer = null;
			BuyingOrder removeOrder = null;
			Query query = em.createNativeQuery( "select from Customer where name='Ivahoe'", Customer.class );
			customer = (Customer) query.getResultList().get( 0 );
			list = new LinkedList<>();
			for ( BuyingOrder buyingOrder : customer.getOrders() ) {
				if ( !buyingOrder.getOrderKey().equals( "4433" ) ) {
					list.add( buyingOrder );
				}
				else {
					removeOrder = buyingOrder;
					log.debug( "RemovedOrder: "+ removeOrder.getbKey() );
				}
			}
			log.debug(  MessageFormat.format("Orders size. old: {0}; new:{1}",customer.getOrders().size(), list.size() ));
			customer.setOrders( list );
			em.merge( customer );
			removeOrder.setOwner( null );
			em.merge( removeOrder );
			em.getTransaction().commit();
			em.clear();

			em.getTransaction().begin();
			query = em.createNativeQuery( "select from Customer where name='Ivahoe'", Customer.class );
			customer = (Customer) query.getResultList().get( 0 );
			list = customer.getOrders();
			assertEquals( 2l, list.size() );
			em.getTransaction().commit();

		}
		catch (Exception e) {
			log.error(  "Error", e );
			em.getTransaction().rollback();
			throw e;
		}
	}

	@Test
	public void test4ReadAllAssociations() throws Exception {
		log.debug(  "start" );
		try {
			em.getTransaction().begin();
			Query query = em.createNativeQuery( "select from Customer where name='Ivahoe'", Customer.class );
			List<Customer> customers = query.getResultList();
			log.debug(  "customers.size(): "+ customers.size() );
			assertFalse( "Customers must be", customers.isEmpty() );
			Customer customer = customers.get( 0 );
			log.debug(  MessageFormat.format( "use Customer with id {0} ( rid: {1} )",customer.getbKey(), customer.getRid() ) );
			assertNotNull( "Customer with 'Ivahoe' must be saved!", customer );
			assertFalse( "Customer must to have orders!", customer.getOrders().isEmpty() );
			// assertFalse("Customer must to have phones!", customer.getPhones().isEmpty());
			Set<String> orderKeySet = new HashSet<>();
			log.debug(  "orders :"+ customer.getOrders().size() );
			for ( BuyingOrder order : customer.getOrders() ) {
				log.debug(  MessageFormat.format(  "order.orderKey:{0}; id: {1}",
						order.getOrderKey(), order.getbKey() ) );
				orderKeySet.add( order.getOrderKey() );
			}
			log.debug( "OrderKeys : "+ orderKeySet );
			assertTrue( "OrderKey 2233 must be linked!", orderKeySet.contains( "2233" ) );

			BuyingOrder order = customer.getOrders().get( 0 );
			assertNotNull( "Order with id '" + order.getbKey() + "' must to have owner!", order.getOwner() );

			em.getTransaction().commit();
		}
		catch (Exception e) {
			log.error( "Error", e );
			em.getTransaction().rollback();
			throw e;
		}
	}

}
