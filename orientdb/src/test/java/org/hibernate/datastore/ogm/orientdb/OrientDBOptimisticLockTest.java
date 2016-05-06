/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.datastore.ogm.orientdb;

import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import static org.hibernate.datastore.ogm.orientdb.OrientDBSimpleTest.MEMORY_TEST;

import java.util.Calendar;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.FlushModeType;
import javax.persistence.OptimisticLockException;
import javax.persistence.Persistence;
import javax.persistence.Query;
import javax.persistence.RollbackException;
import org.apache.log4j.Logger;

import org.hibernate.datastore.ogm.orientdb.jpa.Writer;
import org.hibernate.datastore.ogm.orientdb.utils.MemoryDBUtil;
import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/**
 * @author Sergey Chernolyas &lt;sergey.chernolyas@gmail.com&gt;
 * @see https://blogs.oracle.com/enterprisetechtips/entry/locking_and_concurrency_in_java
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
		Writer writer = null;
		try {
			log.info( "Create first writer" );
			em.getTransaction().begin();
			writer = new Writer();
			writer.setbKey( 1L );
			writer.setCount( 1L );
			writer.setName( "Valter Scott" );
			Calendar calendar = Calendar.getInstance();
			calendar.set( 1771, 11, 15 );
			writer.setBirthDate( calendar.getTime() );
			em.persist( writer );
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

		log.info( "waiting results...." );
		ForkJoinTask<Long> t1 = ForkJoinPool.commonPool().submit( ForkJoinTask.adapt( new WriterUpdateThread( 2, 1, emf.createEntityManager() ) ) );
		ForkJoinTask<Long> t2 = ForkJoinPool.commonPool().submit( ForkJoinTask.adapt( new WriterUpdateThread( 3, 1, emf.createEntityManager() ) ) );

		long t1Result = -1;
		long t2Result = -1;
		try {
			t1Result = t1.get();
			log.info( "t1 result:" + t1Result );
			t2Result = t2.get();
			log.info( "t2 result:" + t2Result );
		}
		catch (ExecutionException e) {
			log.error( "Error in task", e );
			if ( e.getCause() instanceof AssertionError ) {
				throw e;
			}
			RollbackException re = (RollbackException) e.getCause();
			boolean isNotActualVersion = isNotActualVersion( re );
			assertTrue( "Must be right exception (OConcurrentModificationException or HibernateException (id OGM001716)",
					isOConcurrentModificationException( re ) ||
							( isNotActualVersion ) );
		}
		if ( t1.isDone() && t2.isDone() ) {
			if ( isAnyThreadSuccess( t1, t2 ) ) {
				try {
					em.clear();
					em.getTransaction().begin();
					writer = em.find( Writer.class, 1l );
					assertTrue( "Counter must be changed!", writer.getCount() > 1L ); // one thread commited change
					assertEquals( "Name must be uppercase!", "Valter Scott".toUpperCase(), writer.getName() );
					em.getTransaction().commit();
				}
				catch (Exception e) {
					log.error( "Error", e );
					em.getTransaction().rollback();
					throw e;
				}
			}
			else {
				assertTrue( "No success threads!", false );
			}
		}
	}

	@Test
	public void test2ParallelUpdateDeleteEntity() throws Exception {
		Writer writer = null;
		try {
			log.info( "Create second writer" );
			em.getTransaction().begin();
			writer = new Writer();
			writer.setbKey( 2L );
			writer.setCount( 1L );
			writer.setName( "Agniya Barto" );
			Calendar calendar = Calendar.getInstance();
			calendar.set( 1906, 01, 4 );
			writer.setBirthDate( calendar.getTime() );
			em.persist( writer );
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

		log.info( "waiting results...." );
		ForkJoinTask<Long> t1 = ForkJoinPool.commonPool().submit( ForkJoinTask.adapt( new WriterUpdateThread( 2, 2, emf.createEntityManager() ) ) );
		ForkJoinTask<Long> t2 = ForkJoinPool.commonPool().submit( ForkJoinTask.adapt( new WriterDeleteThread( 3, 2, emf.createEntityManager() ) ) );

		long t1Result = -1;
		long t2Result = -1;
		boolean isOrientDBEx = false;
		boolean isOptimisticLockEx = false;
		boolean isNotActualVersion = false;
		try {
			t1Result = t1.get();
			log.info( "t1 result:" + t1Result );
			t2Result = t2.get();
			log.info( "t2 result:" + t2Result );
		}
		catch (ExecutionException e) {
			log.error( "Error in task", e );
			if ( e.getCause() instanceof AssertionError ) {
				throw e;
			}
			RollbackException re = (RollbackException) e.getCause();
			isOrientDBEx = isOConcurrentModificationException( re );
			isOptimisticLockEx = isOptimisticLockException( re );
			isNotActualVersion = isNotActualVersion( re );
			assertTrue( "Must be right exception (OConcurrentModificationException or OptimisticLockException or HibernateException (id OGM001716) )",
					( isOrientDBEx || isOptimisticLockEx || isNotActualVersion ) );
		}
		if ( t1.isDone() && t2.isDone() ) {
			if ( isAnyThreadSuccess( t1, t2 ) ) {
				try {
					log.debug( String.format( "Exception flags: OConcurrentModificationException: %s;OptimisticLockException:%s;HibernateException:%s",
							isOrientDBEx, isOptimisticLockEx, isNotActualVersion ) );
					em.clear();
					em.getTransaction().begin();
					writer = em.find( Writer.class, 2l );
					if ( !isOrientDBEx ) {
						// delete thread get exception and not delete entity
						assertNull( "Writer must be deleted!", writer );
					}
					em.getTransaction().commit();
				}
				catch (Exception e) {
					log.error( "Error", e );
					em.getTransaction().rollback();
					throw e;
				}
			}
			else {
				assertTrue( "No success threads!", false );
			}
		}
	}

	private static boolean isNotActualVersion(RollbackException re) {
		boolean isNotActualVersion = false;
		try {
			isNotActualVersion = ( re.getCause().getCause().getCause().getMessage().contains( "OGM001716" ) );
			log.info( "re.getCause().getCause().getClass():" + re.getCause().getCause().getCause().getClass() );
		}
		catch (NullPointerException e1) {
		}
		return isNotActualVersion;
	}

	private static boolean isOptimisticLockException(RollbackException re) {
		return re.getCause().getCause() instanceof OptimisticLockException;
	}

	private static boolean isOConcurrentModificationException(RollbackException re) {
		return re.getCause().getCause() instanceof OConcurrentModificationException;
	}

	private boolean isAnyThreadSuccess(ForkJoinTask<Long> t1, ForkJoinTask<Long> t2) {
		return t1.isCompletedNormally() || t2.isCompletedNormally();
	}

	private class WriterUpdateThread implements Callable<Long> {

		private final Logger log = Logger.getLogger( WriterUpdateThread.class.getName() );
		private final long taskId;
		private final long writerId;
		private final EntityManager localEm;

		public WriterUpdateThread(long taskId, long writerId, EntityManager localEm) {
			this.taskId = taskId;
			this.writerId = writerId;
			this.localEm = localEm;
		}

		@Override
		public Long call() throws Exception {
			try {
				log.info( "begin reading..." );
				localEm.getTransaction().begin();
				Query query = localEm.createNativeQuery( "select from writer where bKey=" + writerId, Writer.class );
				List<Writer> results = query.getResultList();
				assertFalse( "Writer must be!", results.isEmpty() );
				Writer writer = results.get( 0 );
				writer.setCount( taskId );
				writer = localEm.merge( writer );

				log.info( "begin writing...." );
				localEm.getTransaction().commit();
				log.info( "transaction commited" );
			}
			catch (Exception e) {
				if ( localEm.getTransaction().isActive() ) {
					log.info( "try to rollback transaction" );
					localEm.getTransaction().rollback();
				}
				throw e;
			}
			finally {
				localEm.clear();
				localEm.close();
			}
			return taskId;
		}
	}

	private class WriterDeleteThread implements Callable<Long> {

		private final Logger log = Logger.getLogger( WriterDeleteThread.class.getName() );
		private final long taskId;
		private final long writerId;
		private final EntityManager localEm;

		public WriterDeleteThread(long taskId, long writerId, EntityManager localEm) {
			this.taskId = taskId;
			this.writerId = writerId;
			this.localEm = localEm;
		}

		@Override
		public Long call() throws Exception {
			try {
				log.info( "begin reading..." );
				localEm.getTransaction().begin();
				Query query = localEm.createNativeQuery( "select from writer where bKey=" + writerId, Writer.class );
				List<Writer> results = query.getResultList();
				assertFalse( "Writer must be!", results.isEmpty() );
				Writer writer = results.get( 0 );
				localEm.remove( writer );
				log.info( "begin writing...." );
				localEm.getTransaction().commit();
				log.info( "transaction commited" );
			}
			catch (Exception e) {
				if ( localEm.getTransaction().isActive() ) {
					log.info( "try to rollback transaction" );
					localEm.getTransaction().rollback();
				}
				throw e;
			}
			finally {
				localEm.clear();
				localEm.close();
			}
			return taskId;
		}
	}
}
