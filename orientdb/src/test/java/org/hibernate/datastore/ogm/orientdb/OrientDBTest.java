/*
 * Copyright (C) 2016 Hibernate.
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA 02110-1301  USA
 */
package org.hibernate.datastore.ogm.orientdb;

import com.orientechnologies.orient.core.id.ORecordId;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;
import org.apache.log4j.BasicConfigurator;
import org.hibernate.datastore.ogm.orientdb.jpa.Customer;
import org.hibernate.datastore.ogm.orientdb.util.MemoryDBUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author chernolyassv
 */
public class OrientDBTest {

    private static final Logger LOG = Logger.getLogger(OrientDBTest.class.getName());
    private static Map<String, ORecordId> classIdMap;
    private static EntityManager em;
    private static EntityManagerFactory emf;

    public OrientDBTest() {
    }

    @BeforeClass
    public static void setUpClass() {
        classIdMap = MemoryDBUtil.prepareDb("memory:test");
        BasicConfigurator.configure();
        emf = Persistence.createEntityManagerFactory("hibernateOgmJpaUnit");
        em = emf.createEntityManager();

    }

    @AfterClass
    public static void tearDownClass() {
        em.close();
        emf.close();
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    @Test
    public void find() {
        System.out.println("org.hibernate.datastore.ogm.orientdb.OrientDBTestIT.find()");
        try {
            em.getTransaction().begin();
            ORecordId id = classIdMap.get("Customer");
            Customer customer = em.find(Customer.class, id);
            System.out.println("read entity properties:");
            System.out.println("customer.getId():" + customer.getId());
            System.out.println("customer.getName():" + customer.getName());
            assertEquals(id, customer.getId());
        } finally {
            em.getTransaction().commit();
        }

    }

    @Test
    public void createNativeQuery() {
        System.out.println("org.hibernate.datastore.ogm.orientdb.OrientDBTestIT.createNativeQuery()");
        try {
            em.getTransaction().begin();
            System.out.println("query: select from Customer");
            Query query = em.createNativeQuery("select from Customer", Customer.class);
            List<Customer> customers = query.getResultList();
            assertFalse("Customers must be", customers.isEmpty());
            ORecordId id = classIdMap.get("Customer");

            assertEquals(id, customers.get(0).getId());

            System.out.println("query: select from " + id.toString());
            query = em.createNativeQuery("select from " + id.toString(), Customer.class);
            customers = query.getResultList();
            assertFalse("Customers must be", customers.isEmpty());
            /*
            not supported by OrientDB core
            System.out.println("query: select from Customer where name={name}");
            query = em.createNativeQuery("select from Customer where name={name}", Customer.class);
            query.setParameter("name", "Ivahoe");
            customers = query.getResultList();
            assertFalse("Customers must be", customers.isEmpty());
             */
            
            System.out.println("query: select from Customer where name='Ivahoe'");
            query = em.createNativeQuery("select from Customer where name='Ivahoe'", Customer.class);
            customers = query.getResultList();
            assertFalse("Customers must be", customers.isEmpty());

        } finally {
            em.getTransaction().commit();
        }

    }

}
