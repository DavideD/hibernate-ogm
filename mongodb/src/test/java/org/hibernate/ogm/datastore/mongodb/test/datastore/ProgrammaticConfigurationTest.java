/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.mongodb.test.datastore;

import static org.fest.assertions.Assertions.*;

import java.util.Map;
import java.util.Map.Entry;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.boot.MetadataSources;
import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.AvailableSettings;
import org.hibernate.ogm.boot.OgmSessionFactoryBuilder;
import org.hibernate.ogm.cfg.OgmProperties;
import org.hibernate.ogm.datastore.mongodb.MongoDB;
import org.hibernate.ogm.datastore.mongodb.utils.MongoDBTestHelper;
import org.junit.Test;

/**
 * @author Davide D'Alto
 */
public class ProgrammaticConfigurationTest {

	private final String ID = "ichi2";

	@Test
	public void testConfiguration() throws Exception {
		StandardServiceRegistryBuilder serviceRegistryBuilder = new StandardServiceRegistryBuilder();
		serviceRegistryBuilder.applySetting( OgmProperties.ENABLED, true );
		serviceRegistryBuilder.applySetting( OgmProperties.DATASTORE_PROVIDER, MongoDB.DATASTORE_PROVIDER_NAME );
		serviceRegistryBuilder.applySetting( OgmProperties.CREATE_DATABASE, false );
		serviceRegistryBuilder.applySetting( AvailableSettings.ENABLE_LAZY_LOAD_NO_TRANS, true );
		applyConnectionSettings( serviceRegistryBuilder );
		StandardServiceRegistry serviceRegistry = serviceRegistryBuilder.build();

		SessionFactory sessionFactory = new MetadataSources( serviceRegistry )
				.addAnnotatedClass( Example.class )
				.buildMetadata()
				.getSessionFactoryBuilder()
				.unwrap( OgmSessionFactoryBuilder.class )
				.build();

		try ( Session session = sessionFactory.openSession() ) {
			Transaction tx = session.beginTransaction();
			session.persist( new Example( ID, "Example well done" ) );
			tx.commit();

			tx = session.beginTransaction();
			Example example = session.get( Example.class, ID );
			assertThat( example ).isNotNull();
			tx.commit();

		}
		finally {
			sessionFactory.close();
		}
	}

	private void applyConnectionSettings(StandardServiceRegistryBuilder serviceRegistryBuilder) {
		Map<String, String> settings = MongoDBTestHelper.getMongoDBSettings();
		for ( Entry<String, String> setting : settings.entrySet() ) {
			serviceRegistryBuilder.applySetting( setting.getKey(), setting.getValue() );
		}
	}

	@Entity
	@Table(name = "Example")
	static class Example {

		@Id
		private String id;
		private String description;

		public Example() {
		}

		public Example(String id, String description) {
			this.id = id;
			this.description = description;
		}

		public String getId() {
			return id;
		}

		public void setId(String id) {
			this.id = id;
		}

		public String getDescription() {
			return description;
		}

		public void setDescription(String description) {
			this.description = description;
		}
	}
}
