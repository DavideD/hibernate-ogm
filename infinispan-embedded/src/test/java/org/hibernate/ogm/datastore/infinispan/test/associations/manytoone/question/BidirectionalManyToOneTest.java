/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.infinispan.test.associations.manytoone.question;

import static org.fest.assertions.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

import org.fest.util.Files;
import org.hibernate.cfg.AvailableSettings;
import org.hibernate.ogm.OgmSession;
import org.hibernate.ogm.OgmSessionFactory;
import org.hibernate.ogm.datastore.infinispan.InfinispanProperties;
import org.hibernate.ogm.datastore.keyvalue.options.CacheMappingType;
import org.hibernate.ogm.utils.OgmTestCase;
import org.hibernate.ogm.utils.TestSessionFactory;
import org.hibernate.ogm.utils.TestSessionFactory.Scope;
import org.junit.AfterClass;
import org.junit.Test;

/**
 * Test associations loading when infinispan embedded is used with file stored on the
 * disk.
 * <p>
 * NOTE: This class makes sense only if the tests are executed in order:
 * <ol>
 * <li>create the file by persisting the entities</li>
 * <li>read from the file</li>
 * </ol>
 * </p>
 * <p>The error occurs only when we use different session factories, I susspose that the values
 * get cached otherwise.
 * </p>
 */
public class BidirectionalManyToOneTest extends OgmTestCase {

	// The property in hibernate.properties containing the path to the created
	// directory for the infinispan file-store
	private static final String FILE_STORE_PROPERTY = "test.infinispan.file-store";
	@TestSessionFactory(scope = Scope.TEST_METHOD)
	protected OgmSessionFactory factory;

	@Override
	protected OgmSession openSession() {
		super.sessionFactory = factory;
		return super.openSession();
	}

	String name = "prova";
	String id_foo = "Foo_" + name + "_id";
	String id_bar = "Bar_" + id_foo;

	@Override
	protected void configure(Map<String, Object> cfg) {
		super.configure(cfg);
		cfg.put(AvailableSettings.HBM2DDL_AUTO, "create");
		cfg.put(InfinispanProperties.CACHE_MAPPING, CacheMappingType.CACHE_PER_KIND);
		cfg.put(InfinispanProperties.CONFIGURATION_RESOURCE_NAME, "ems-model-infinispan.xml");
	}

	@Test
	public void test1_FileCreation() throws Exception {
		Foo foo = new Foo();
		foo.setId(id_foo);
		foo.setName("Foo_" + name);

		Bar bar = new Bar();
		bar.setName("Bar_" + name);
		bar.setId(id_bar);

		bar.setFoo(foo);
		foo.setBars(Arrays.asList(bar));
		inTransaction(session -> session.persist(foo));

		inTransaction(session -> {
			Foo result = session.get(Foo.class, id_foo);
			assertThat(result).isNotNull();
			assertThat(result.getBars()).hasSize(1);
			assertThat(result.getBars().get(0)).isEqualTo(bar);
		});
	}

	@Test
	public void test2_ReadFromFile() throws Exception {
		Bar bar = new Bar();
		bar.setName("Bar_" + name);
		bar.setId(id_bar);

		inTransaction(session -> {
			Foo result = session.get(Foo.class, id_foo);
			assertThat(result).isNotNull();
			assertThat(result.getBars()).hasSize(1);
			assertThat(result.getBars().get(0)).isEqualTo(bar);
		});
	}

	@AfterClass
	public static void removeFileStored() {
		String fileStorePath = fileStorePath();
		Files.delete( new File( fileStorePath ) );
	}

	private static String fileStorePath() {
		Properties hibernateProperties = new Properties();
		final InputStream resourceAsStream = BidirectionalManyToOneTest.class.getClassLoader().getResourceAsStream( "hibernate.properties" );
		try {
			hibernateProperties.load( resourceAsStream );
		}
		catch (IOException e) {
			throw new RuntimeException( e );
		}
		finally {
			try {
				resourceAsStream.close();
			}
			catch (IOException e) {
				throw new RuntimeException( e );
			}
		}
		return hibernateProperties.getProperty(FILE_STORE_PROPERTY);
	}

	@Override
	protected Class<?>[] getAnnotatedClasses() {
		return new Class<?>[] { Foo.class, Bar.class };
	}
}
