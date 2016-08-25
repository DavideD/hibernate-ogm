/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.neo4j.bolt.impl;

import static org.hibernate.ogm.datastore.neo4j.query.parsing.cypherdsl.impl.CypherDSL.escapeIdentifier;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.hibernate.boot.model.relational.Database;
import org.hibernate.boot.model.relational.Sequence;
import org.hibernate.cfg.Environment;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.ogm.datastore.neo4j.bolt.dialect.impl.BoltStatementsRunner;
import org.hibernate.ogm.datastore.neo4j.impl.BaseNeo4jSchemaDefiner;
import org.hibernate.ogm.datastore.neo4j.logging.impl.Log;
import org.hibernate.ogm.datastore.neo4j.logging.impl.LoggerFactory;
import org.hibernate.ogm.datastore.spi.DatastoreProvider;
import org.hibernate.ogm.model.key.spi.IdSourceKeyMetadata;
import org.hibernate.service.spi.ServiceRegistryImplementor;
import org.hibernate.tool.hbm2ddl.UniqueConstraintSchemaUpdateStrategy;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.util.Resource;
import org.neo4j.graphdb.Label;

/**
 * Initialize the schema for the Neo4j database:
 * <ol>
 * <li>create sequences;</li>
 * <li>create unique constraints on identifiers, natural ids and unique columns</li>
 * </ol>
 * <p>
 * Note that unique constraints involving multiple columns won't be applied because Neo4j does not support it.
 * <p>
 * The creation of unique constraints can be skipped setting the property
 * {@link Environment#UNIQUE_CONSTRAINT_SCHEMA_UPDATE_STRATEGY} to the value
 * {@link UniqueConstraintSchemaUpdateStrategy#SKIP}. Because in Neo4j unique constraints don't have a name, setting the
 * value to {@link UniqueConstraintSchemaUpdateStrategy#RECREATE_QUIETLY} or
 * {@link UniqueConstraintSchemaUpdateStrategy#DROP_RECREATE_QUIETLY} will have the same effect: keep the existing
 * constraints and create the missing one.
 *
 * @author Davide D'Alto
 * @author Gunnar Morling
 */
public class BoltNeo4jSchemaDefiner extends BaseNeo4jSchemaDefiner<List<Statement>> {

	private static final Log log = LoggerFactory.getLogger();

	@Override
	public void initializeSchema(SchemaDefinitionContext context) {
		SessionFactoryImplementor sessionFactoryImplementor = context.getSessionFactory();
		ServiceRegistryImplementor registry = sessionFactoryImplementor.getServiceRegistry();

		BoltNeo4jDatastoreProvider provider = (BoltNeo4jDatastoreProvider) registry.getService( DatastoreProvider.class );
		createSequences( context.getDatabase(), context.getAllIdSourceKeyMetadata(), provider );
		createEntityConstraints( provider.getDriver(), context.getDatabase(), sessionFactoryImplementor.getProperties() );
	}

	private void createSequences(Database database, Iterable<IdSourceKeyMetadata> idSourceKeyMetadata, BoltNeo4jDatastoreProvider provider) {
		List<Sequence> sequences = sequences( database );

		List<Statement> constraintStatements = new ArrayList<Statement>();
		provider.getSequenceGenerator().createSequencesConstraints( constraintStatements, sequences );
		provider.getSequenceGenerator().createUniqueConstraintsForTableSequences( constraintStatements, idSourceKeyMetadata );

		Driver driver = provider.getDriver();
		Session session = null;
		try {
			session = driver.session();
			Transaction tx = null;
			try {
				tx = session.beginTransaction();
				BoltStatementsRunner.runAll( tx, constraintStatements );
				tx.success();
			}
			finally {
				close( tx );
			}

			// We create the sequences in a separate transaction because
			// Neo4j does not allow the creation of constraints and graph elements in the same transaction
			List<Statement> sequenceStatements = new ArrayList<>();
			provider.getSequenceGenerator().createSequences( sequenceStatements, sequences );
			try {
				tx = session.beginTransaction();
				BoltStatementsRunner.runAll( tx, sequenceStatements );
				tx.success();
			}
			finally {
				close( tx );
			}
		}
		finally {
			close( session );
		}
	}

	private void close(Resource closable) {
		if ( closable != null ) {
			closable.close();
		}
	}

	private void createEntityConstraints(Driver remoteNeo4j, Database database, Properties properties) {
		UniqueConstraintSchemaUpdateStrategy constraintMethod = UniqueConstraintSchemaUpdateStrategy.interpret( properties.get(
				Environment.UNIQUE_CONSTRAINT_SCHEMA_UPDATE_STRATEGY )
		);

		log.debugf( "%1$s property set to %2$s" , Environment.UNIQUE_CONSTRAINT_SCHEMA_UPDATE_STRATEGY, constraintMethod );
		if ( constraintMethod == UniqueConstraintSchemaUpdateStrategy.SKIP ) {
			log.tracef( "Skipping generation of unique constraints" );
		}
		else {
			List<Statement> statements = new ArrayList<Statement>();
			addUniqueConstraints( statements, database );
			log.debug( "Creating missing constraints" );
			Session session = null;
			try {
				session = remoteNeo4j.session();
				Transaction tx = null;
				try {
					tx = session.beginTransaction();
					BoltStatementsRunner.runAll( tx, statements );
					tx.success();
				}
				catch (ClientException e) {
					throw log.constraintsCreationException( e.neo4jErrorCode(), e.getMessage() );
				}
				finally {
					close( tx );
				}
			}
			finally {
				close( session );
			}
		}
	}

	@Override
	protected void createUniqueConstraintIfMissing(List<Statement> statements, Label label, String property) {
		log.tracef( "Creating unique constraint for nodes labeled as %1$s on property %2$s", label, property );
		StringBuilder queryBuilder = new StringBuilder( "CREATE CONSTRAINT ON (n:" );
		escapeIdentifier( queryBuilder, label.name() );
		queryBuilder.append( ") ASSERT n." );
		escapeIdentifier( queryBuilder, property );
		queryBuilder.append( " IS UNIQUE" );
		statements.add( new Statement( queryBuilder.toString() ) );
	}
}
