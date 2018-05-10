package org.hibernate.ogm.datastore.mongodb.test;

import org.hibernate.SessionFactory;
import org.hibernate.ogm.cfg.OgmConfiguration;
import org.hibernate.ogm.cfg.OgmProperties;
import org.hibernate.ogm.datastore.impl.DatastoreProviderType;
import org.hibernate.ogm.datastore.mongodb.MongoDB;
import org.hibernate.ogm.datastore.mongodb.MongoDBProperties;
import org.hibernate.ogm.datastore.mongodb.options.AuthenticationMechanismType;
import org.hibernate.ogm.datastore.mongodb.options.WriteConcernType;
import org.junit.Test;

public class MongoDBTest {

	@Test
	public void test() {
		OgmConfiguration configuration = new OgmConfiguration();

		configuration.configureOptionsFor( MongoDB.class )
			.writeConcern( WriteConcernType.UNACKNOWLEDGED );

		SessionFactory sf = configuration
			.setProperty( OgmProperties.DATABASE, "mongodb_database" )
			.setProperty( OgmProperties.DATASTORE_PROVIDER, DatastoreProviderType.MONGODB.name() )
			// All this properties are optional, appropriate default will be used if missing
			.setProperty( OgmProperties.CREATE_DATABASE, "false" )
			.setProperty( OgmProperties.USERNAME, "username" )
			.setProperty( OgmProperties.PASSWORD, "password" )
			.setProperty( OgmProperties.HOST, "localhost:12897" )
			.setProperty( MongoDBProperties.AUTHENTICATION_MECHANISM, AuthenticationMechanismType.BEST.name() )

//			.addAnnotatedClass org.hsnr.rest.domain.entities.Address.class )
			.buildSessionFactory();
		sf.close();
	}
}
