/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.datastore.ogm.orientdb;

import java.util.Calendar;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.FlushModeType;
import javax.persistence.Persistence;
import org.junit.FixMethodOrder;
import org.junit.runners.MethodSorters;
import org.apache.log4j.Logger;
import static org.hibernate.datastore.ogm.orientdb.OrientDBSimpleTest.MEMORY_TEST;
import org.hibernate.datastore.ogm.orientdb.jpa.Writer;
import org.hibernate.datastore.ogm.orientdb.utils.MemoryDBUtil;
import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author Sergey Chernolyas <sergey.chernolyas@gmail.com>
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class OrientDBOptimisticLockTest {

	private static final Logger log = Logger.getLogger( OrientDBOptimisticLockTest.class.getName() );
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
	public void test1ParallelUpdateEntity() throws Exception {
		Writer valterScott = null;
		try {
			log.info( "Create first writer" );
			em.getTransaction().begin();
			valterScott = new Writer();
			valterScott.setbKey( 1L );
			valterScott.setCount( 1L );
			valterScott.setName( "Valter Scott" );
			Calendar calendar = Calendar.getInstance();
			calendar.set( 1771, 11, 15 );
			valterScott.setBirthDate( calendar.getTime() );
			em.persist( valterScott );
			em.getTransaction().commit();
			log.info( "Writer persisted" );
			em.clear();
		}
		catch (Exception e) {
			log.error( "Error", e );
			if ( em.getTransaction().isActive() ) {
				em.getTransaction().rollback();
			}
			throw e;
		}

		ForkJoinTask<Long> t1 = ForkJoinPool.commonPool().submit( ForkJoinTask.adapt( new WriterUpdateThread( 2 ) ) );
		// ForkJoinTask<Long> t2 = ForkJoinPool.commonPool().submit(ForkJoinTask.adapt(new WriterUpdateThread(3)));
		log.info( "waiting results...." );
		long t1Result = -1;
		long t2Result = -1;
		try {
			t1Result = t1.get();
			// t2Result = t2.get();
			log.info( "t1 result:" + t1Result );
			// log.info("t2 result:" + t2Result);
		}
		catch (ExecutionException e) {
			log.error( "Error in task", e );
		}
		// if (t1.isDone() && t2.isDone()) {
		if ( t1.isDone() ) {
			try {
				em.clear();
				em.getTransaction().begin();
				valterScott = em.find( Writer.class, 1l );
				log.info( "valterScott.getCount(): " + valterScott.getCount() );
				log.info( "valterScott.getName(): " + valterScott.getName() );
				assertTrue( "Counter must be changed!", valterScott.getCount() > 1L );
				assertEquals( "Name must be uppercase!", "Valter Scott".toUpperCase(), valterScott.getName() );
				em.getTransaction().commit();
			}
			catch (Exception e) {
				log.error( "Error", e );
				em.getTransaction().rollback();
				throw e;
			}
		}
	}

	private class WriterUpdateThread implements Callable<Long> {

		private final Logger log = Logger.getLogger( WriterUpdateThread.class.getName() );
		private long taskId;

		public WriterUpdateThread(long taskId) {
			this.taskId = taskId;
		}

		@Override
		public Long call() throws Exception {
			try {
				log.info( "begin reading" );
				em.getTransaction().begin();
				Writer valterScott = em.find( Writer.class, 1l );
				valterScott.setCount( taskId );
				valterScott = em.merge( valterScott );
				log.info( "begin writing...." );
				em.getTransaction().commit();
				em.clear();
				log.info( "transaction commited" );
			}
			catch (Exception e) {
				log.error( "Error", e );
				if ( em.getTransaction().isActive() ) {
					log.info( "try to rollback transaction" );
					em.getTransaction().rollback();
				}
				throw e;
			}
			return taskId;
		}
	}
}
