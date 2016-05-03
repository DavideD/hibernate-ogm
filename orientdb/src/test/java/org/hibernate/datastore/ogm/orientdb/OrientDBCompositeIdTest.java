/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.datastore.ogm.orientdb;

import java.util.List;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.FlushModeType;
import javax.persistence.Persistence;
import javax.persistence.Query;
import org.apache.log4j.Logger;
import static org.hibernate.datastore.ogm.orientdb.OrientDBSimpleTest.MEMORY_TEST;
import org.hibernate.datastore.ogm.orientdb.jpa.Passport;
import org.hibernate.datastore.ogm.orientdb.jpa.PassportPK;
import org.hibernate.datastore.ogm.orientdb.utils.MemoryDBUtil;
import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/**
 * @author Sergey Chernolyas <sergey.chernolyas@gmail.com>
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class OrientDBCompositeIdTest {

	private static final Logger log = Logger.getLogger( OrientDBCompositeIdTest.class.getName() );
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
		if ( emf != null && em != null ) {
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
	public void test1InsertNewPassport() {
		log.debug( "start" );
		try {
			em.getTransaction().begin();
			Passport newPassport = new Passport();
			newPassport.setFio( "fio1" );
			newPassport.setSeria( 6002 );
			newPassport.setNumber( 11111111 );
			log.debug( "New Passport ready for  persit" );
			em.persist( newPassport );
			em.getTransaction().commit();
			em.clear();
		}
		catch (Exception e) {
			log.error( "Error", e );
			em.getTransaction().rollback();
			throw e;
		}

		try {

			em.getTransaction().begin();
			PassportPK pk = new PassportPK();
			pk.setNumber( 11111111 );
			pk.setSeria( 6002 );
			Passport passport = em.find( Passport.class, pk );
			assertNotNull( "Passport must be saved!", passport );
			assertNotNull( "Passport must have a seria!", passport.getSeria() );
			assertEquals( "Seria must be a 6002", 6002, passport.getSeria() );
                        assertEquals( "Seria must be a 11111111", 11111111, passport.getNumber() );
                        assertEquals( "Seria must be a 'fio1'", "fio1", passport.getFio() );
			em.getTransaction().commit();
		}
		catch (Exception e) {
			log.error( "Error", e );
			em.getTransaction().rollback();
			throw e;
		}

	}
        
        @Test
	public void test2UpdatePassport() {
		log.debug( "start" );
                PassportPK pk = new PassportPK();
		pk.setNumber( 11111111 );
		pk.setSeria( 6002 );
		try {

			em.getTransaction().begin();
			Passport passport = em.find( Passport.class, pk );
			assertNotNull( "Passport must be saved!", passport );
			assertNotNull( "Passport must have a seria!", passport.getSeria() );
			assertEquals( "Seria must be a 6002", 6002, passport.getSeria() );
                        passport.setFio("fio2");
                        em.merge(passport);
			em.getTransaction().commit();
		}
		catch (Exception e) {
			log.error( "Error", e );
                        if (em.getTransaction().isActive()) {
                            em.getTransaction().rollback();
                        }
			throw e;
		} finally {
                    em.clear();
                }
                try {
			em.getTransaction().begin();			
			Passport passport = em.find( Passport.class, pk );
			assertNotNull( "Passport must be saved!", passport );
			assertNotNull( "Passport must have a seria!", passport.getSeria() );
			assertEquals( "Seria must be a 6002", 6002, passport.getSeria() );
                        assertEquals( "Seria must be a 11111111", 11111111, passport.getNumber() );
                        assertEquals( "Seria must be a 'fio2'", "fio2", passport.getFio() );
			em.getTransaction().commit();
		}
		catch (Exception e) {
			log.error( "Error", e );
			if (em.getTransaction().isActive()) {
                            em.getTransaction().rollback();
                        }
			throw e;
		}
	}
        
        @Test
	public void test3SearchByNativeQuery() {
		log.debug( "start" );
                PassportPK pk1 = new PassportPK();
		pk1.setNumber( 11111111 );
		pk1.setSeria( 6002 );
                PassportPK pk2 = new PassportPK();
		pk2.setNumber( 22222222 );
		pk2.setSeria( 6002 );
		try {

			em.getTransaction().begin();
			Passport newPassport = new Passport();
			newPassport.setFio( "fio3" );
			newPassport.setSeria( pk2.getSeria() );
			newPassport.setNumber( pk2.getNumber() );
			log.debug( "New Passport ready for  persit" );
			em.persist( newPassport );			
			em.getTransaction().commit();
		}
		catch (Exception e) {
			log.error( "Error", e );
                        if (em.getTransaction().isActive()) {
                            em.getTransaction().rollback();
                        }
			throw e;
		} finally {
                    em.clear();
                }
                try {
			em.getTransaction().begin();			
			Query query = em.createNativeQuery("select from Passport where seria=:seria", Passport.class);
                        query.setParameter("seria", pk1.getSeria());
                        List<Passport>  passports = query.getResultList();
                        assertEquals("Must be 2 passports!", 2, passports.size());
			em.getTransaction().commit();
		}
		catch (Exception e) {
			log.error( "Error", e );
			if (em.getTransaction().isActive()) {
                            em.getTransaction().rollback();
                        }
			throw e;
		}
	}

}
