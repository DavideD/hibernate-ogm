/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.datastore.ogm.orientdb;

import com.orientechnologies.orient.core.id.ORecordId;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.FlushModeType;
import javax.persistence.Persistence;
import javax.persistence.Query;
import javax.persistence.TypedQuery;
import org.apache.log4j.BasicConfigurator;
import org.hibernate.CacheMode;
import org.hibernate.datastore.ogm.orientdb.jpa.BuyingOrder;
import org.hibernate.datastore.ogm.orientdb.jpa.Customer;
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
 *
 * @author Sergey Chernolyas (sergey.chernolyas@gmail.com)
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class OrientDBTest {

    private static final Logger LOG = Logger.getLogger(OrientDBTest.class.getName());
    private static Map<String, List<ORecordId>> classIdMap;
    private static EntityManager em;
    private static EntityManagerFactory emf;

    public OrientDBTest() {
    }

    @BeforeClass
    public static void setUpClass() {
        classIdMap = MemoryDBUtil.prepareDb("memory:test");
        //classIdMap = MemoryDBUtil.prepareDb("remote:localhost/pizza");
        BasicConfigurator.configure();
        emf = Persistence.createEntityManagerFactory("hibernateOgmJpaUnit");
        em = emf.createEntityManager();
        em.setFlushMode(FlushModeType.COMMIT);

    }

    @AfterClass
    public static void tearDownClass() {
        if (em != null) {
            em.close();
            emf.close();
        };
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    @Test
    public void test1Find() {
        try {
            em.getTransaction().begin();
            ORecordId id = classIdMap.get("Customer").get(0);
            Customer customer = em.find(Customer.class, id);
            LOG.log(Level.INFO, "read entity properties:");
            LOG.log(Level.INFO, "customer.getId():{0}", customer.getId());
            LOG.log(Level.INFO, "customer.getName(): {0}", customer.getName());
            assertEquals(id, customer.getId());
            
            List<BuyingOrder> orders = customer.getOrders();
        } finally {
            em.getTransaction().commit();
        }

    }

    @Test
    public void test2CreateNativeQuery() {
        try {
            em.getTransaction().begin();
            LOG.log(Level.INFO, "query: select from Customer");
            Query query = em.createNativeQuery("select from Customer", Customer.class);
            List<Customer> customers = query.getResultList();
            assertFalse("Customers must be", customers.isEmpty());
            ORecordId id = classIdMap.get("Customer").get(0);

            assertEquals(id, customers.get(0).getId());

            LOG.log(Level.INFO, "query: select from " + id.toString());
            query = em.createNativeQuery("select from " + id.toString(), Customer.class);
            customers = query.getResultList();
            assertFalse("Customers must be", customers.isEmpty());

            LOG.log(Level.INFO, "query: select from Customer where name=:name");
            query = em.createNativeQuery("select from Customer where name=:name", Customer.class);
            query.setParameter("name", "Ivahoe");
            customers = query.getResultList();
            assertFalse("Customers must be", customers.isEmpty());

            LOG.log(Level.INFO, "query: select from Customer where name='Ivahoe'");
            query = em.createNativeQuery("select from Customer where name='Ivahoe'", Customer.class);
            customers = query.getResultList();
            assertFalse("Customers must be", customers.isEmpty());

        } finally {
            em.getTransaction().commit();
        }

    }

    @Test
    public void test3InsertNewCustomer() throws Exception {
        System.out.println("org.hibernate.datastore.ogm.orientdb.OrientDBTest.insertNewCustomer()");
        try {
            em.getTransaction().begin();
            Customer newCustomer = new Customer();
            newCustomer.setName("test");
            LOG.log(Level.INFO, "New Customer ready for  persit");
            em.persist(newCustomer);
            em.flush();
            Query query = em.createNativeQuery("select from Customer where name=:name", Customer.class);
            query.setParameter("name", "test");
            List<Customer> customers = query.getResultList();
            LOG.log(Level.INFO, "customers.size():" + customers.size());
            assertFalse("Customers must be", customers.isEmpty());
            Customer testCustomer = customers.get(0);
            assertNotNull("Customer with 'test' must be saved!", testCustomer);
            assertTrue("Customer with 'test' must have valid rid!", testCustomer.getId().isValid());

        } catch (Exception e) {
            LOG.log(Level.SEVERE, "Error", e);
            throw e;
        } finally {
            em.getTransaction().commit();
        }
    }

    @Test
    public void test4UpdateCustomer() {
        try {
            em.getTransaction().begin();
            ORecordId id = classIdMap.get("Customer").get(0);
            Customer customer = em.find(Customer.class, id);
            customer.setName("Ivahoe2");
            int oldVersion = customer.getVersion();
            LOG.log(Level.INFO, "old version:{0}", oldVersion);
            em.merge(customer);
            em.flush();
            Customer newCustomer = em.find(Customer.class, id);
            assertNotNull("Must not be null", newCustomer);
            assertEquals(customer.getId(), newCustomer.getId());
            assertEquals("Ivahoe2", newCustomer.getName());
            int newVersion = newCustomer.getVersion();
            LOG.log(Level.INFO, "new version:{0}", newVersion);
            assertTrue("Version must be chanched", (newVersion > oldVersion));
        } catch (Exception e) {
            LOG.log(Level.SEVERE, "Error", e);
            throw e;
        } finally {
            em.getTransaction().commit();
        }
    }

   @Test
    public void test5RemoveCustomer() {
        try {
            em.getTransaction().begin();
            ORecordId id = classIdMap.get("Customer").get(0);
            Customer customer = em.find(Customer.class, id);
            em.remove(customer);
            em.flush();

            Customer removedCustomer = em.find(Customer.class, id);
            LOG.log(Level.INFO, "customer:{0}", removedCustomer);

            //assertEquals(customer.getId(), newCustomer.getId());
            //assertEquals("Ivahoe2", newCustomer.getName());
        } catch (Exception e) {
            LOG.log(Level.SEVERE, "Error", e);
            throw e;
        } finally {
            em.getTransaction().commit();
        }
    }

    //@Test yet not work
    public void createNamedQuery() throws Exception {
        System.out.println("org.hibernate.datastore.ogm.orientdb.OrientDBTestIT.createNamedQuery()");
        try {
            em.getTransaction().begin();
            FullTextEntityManager fullTextEntityManager = Search.getFullTextEntityManager(em);
            MassIndexer indexer = fullTextEntityManager.createIndexer();
            indexer.cacheMode(CacheMode.REFRESH);
            indexer.startAndWait();

            System.out.println("entities has indexed");

            System.out.println("named query: Customer.findAll");
            TypedQuery<Customer> query = em.createNamedQuery("Customer.findAll", Customer.class);
            List<Customer> customers = query.getResultList();
            System.out.println("1.customers.size(): " + customers.size());

            QueryBuilder qb = fullTextEntityManager.getSearchFactory()
                    .buildQueryBuilder().forEntity(Customer.class).get();

            org.apache.lucene.search.Query luceneQuery = qb.all().createQuery();
            FullTextQuery jpaQuery
                    = fullTextEntityManager.createFullTextQuery(luceneQuery, Customer.class);
            jpaQuery.initializeObjectsWith(ObjectLookupMethod.PERSISTENCE_CONTEXT, DatabaseRetrievalMethod.QUERY);

            customers = jpaQuery.getResultList();
            System.out.println("2.customers.size(): " + customers.size());

            assertFalse("Customers must be", customers.isEmpty());
            ORecordId id = classIdMap.get("Customer").get(0);
            assertEquals(id, customers.get(0).getId());

        } finally {
            em.getTransaction().commit();
        }

    }

}
