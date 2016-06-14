/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.orientdb;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.TimeZone;
import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.FlushModeType;
import javax.persistence.Persistence;
import javax.persistence.Query;
import org.apache.log4j.Logger;
import org.hibernate.ogm.datastore.orientdb.jpa.SimpleTypesEntity;
import org.hibernate.ogm.datastore.orientdb.utils.MemoryDBUtil;
import org.hibernate.ogm.type.impl.EnumType;
import org.hibernate.ogm.type.impl.NumericBooleanType;
import org.hibernate.ogm.type.impl.TimestampType;
import org.hibernate.ogm.type.impl.TrueFalseType;
import org.hibernate.ogm.type.impl.YesNoType;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.junit.runners.Parameterized;
import static org.junit.Assert.assertEquals;

/**
 * Test checks CRUD for entities with associations (with links with other entities)
 *
 * @author Sergey Chernolyas &lt;sergey.chernolyas@gmail.com&gt;
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@RunWith(Parameterized.class)
public class OrientDbQueryParamTest {

	private static final Logger log = Logger.getLogger( OrientDbQueryParamTest.class.getName() );
	private static EntityManager em;
	private static EntityManagerFactory emf;

	@Parameterized.Parameter(value = 0)
	public Class searchByClass;
	@Parameterized.Parameter(value = 1)
	public Object searchByValue;
	@Parameterized.Parameter(value = 2)
	public String paramName;
	@Parameterized.Parameter(value = 3)
	public Integer requiredCount;
	@Parameterized.Parameter(value = 4)
	public SimpleTypesEntity preparedEntity;

	@Parameterized.Parameters(name = "class:{0}")
	public static Iterable<Object[]> prepareData() throws ParseException {
		TimeZone.setDefault( TimeZone.getTimeZone( "UTC" ) );
		List<Object[]> list = new LinkedList<>();

		SimpleTypesEntity entity1 = new SimpleTypesEntity( 1L );
		list.add( new Object[]{ Long.class, 1L, "id", 1, entity1 } );

		SimpleTypesEntity entity2 = new SimpleTypesEntity( 2L );
		entity2.setIntValue( 1 );
		list.add( new Object[]{ Integer.class, 1, "intValue", 1, entity2 } );

		SimpleTypesEntity entity3 = new SimpleTypesEntity( 3L );
		entity3.setShortValue( (short) 1 );
		list.add( new Object[]{ Short.class, (short) 1, "shortValue", 1, entity3 } );

		SimpleTypesEntity entity4 = new SimpleTypesEntity( 4L );
		entity4.setByteValue( (byte) 1 );
		list.add( new Object[]{ Byte.class, (byte) 1, "byteValue", 1, entity4 } );

		SimpleTypesEntity entity5 = new SimpleTypesEntity( 5L );
		entity5.setNumericBooleanValue( Boolean.TRUE );
		list.add( new Object[]{ NumericBooleanType.class, (short) 1, "numericBooleanValue", 1, entity5 } );

		SimpleTypesEntity entity6 = new SimpleTypesEntity( 6L );
		entity6.setYesNoBooleanValue( Boolean.TRUE );
		list.add( new Object[]{ YesNoType.class, 'Y', "yesNoBooleanValue", 1, entity6 } );

		SimpleTypesEntity entity7 = new SimpleTypesEntity( 7L );
		entity7.setTfBooleanValue( Boolean.TRUE );
		list.add( new Object[]{ TrueFalseType.class, 'T', "tfBooleanValue", 1, entity7 } );

		SimpleTypesEntity entity8 = new SimpleTypesEntity( 8L );
		entity8.setE1( SimpleTypesEntity.EnumType.E1 );
		list.add( new Object[]{ EnumType.class, 0, "e1", 1, entity8 } );

		SimpleTypesEntity entity9 = new SimpleTypesEntity( 9L );
		entity9.setE2( SimpleTypesEntity.EnumType.E2 );
		list.add( new Object[]{ EnumType.class, "E2", "e2", 1, entity9 } );

		SimpleDateFormat df1 = new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss z" );
		Date now = df1.parse( df1.format( new Date() ) );
		SimpleTypesEntity entity10 = new SimpleTypesEntity( 10L );
		entity10.setCreatedTimestamp( now );
		list.add( new Object[]{ TimestampType.class, now, "createdTimestamp", 1, entity10 } );

		SimpleDateFormat df2 = new SimpleDateFormat( "yyyy-MM-dd z" );
		Date today = df2.parse( df2.format( new Date() ) );
		SimpleTypesEntity entity11 = new SimpleTypesEntity( 11L );
		entity11.setCreatedDate( today );
		list.add( new Object[]{ TimestampType.class, today, "createdDate", 1, entity11 } );

		return list;
	}

	@Test
	public void testSearchBy() {

		try {
			em.getTransaction().begin();
			em.persist( preparedEntity );
			em.getTransaction().commit();
			em.clear();

			em.getTransaction().begin();

			Query query = em.createNativeQuery( "select from SimpleTypesEntity where " + paramName + "=:" + paramName, SimpleTypesEntity.class );
			query.setParameter( paramName, searchByValue );
			assertEquals( requiredCount, Integer.valueOf( query.getResultList().size() ) );
			em.getTransaction().commit();
		}
		catch (Exception e) {
			log.error( "Error", e );
			em.getTransaction().rollback();
			throw e;
		}

	}

	@BeforeClass
	public static void setUpClass() {
		MemoryDBUtil.createDbFactory( OrientDBSimpleTest.MEMORY_TEST );
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

}
