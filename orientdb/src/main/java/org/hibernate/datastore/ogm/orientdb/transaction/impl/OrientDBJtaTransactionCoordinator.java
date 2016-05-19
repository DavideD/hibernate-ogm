/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.datastore.ogm.orientdb.transaction.impl;

import java.sql.Connection;

import javax.transaction.Status;
import javax.transaction.Synchronization;

import org.hibernate.datastore.ogm.orientdb.impl.OrientDBDatastoreProvider;
import org.hibernate.datastore.ogm.orientdb.logging.impl.Log;
import org.hibernate.datastore.ogm.orientdb.logging.impl.LoggerFactory;
import org.hibernate.ogm.transaction.impl.ForwardingTransactionCoordinator;
import org.hibernate.resource.transaction.TransactionCoordinator;
import org.hibernate.resource.transaction.spi.TransactionStatus;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.tx.OTransaction;
import com.orientechnologies.orient.jdbc.OrientJdbcConnection;

/**
 * @author Sergey Chernolyas &lt;sergey.chernolyas@gmail.com&gt;
 */
public class OrientDBJtaTransactionCoordinator extends ForwardingTransactionCoordinator {

	private static Log log = LoggerFactory.getLogger();
	private OrientDBDatastoreProvider datastoreProvider;
	private OTransaction currentOrientDBTransaction;

	public OrientDBJtaTransactionCoordinator(TransactionCoordinator coordinator, OrientDBDatastoreProvider datastoreProvider) {
		super( coordinator );
		this.datastoreProvider = datastoreProvider;
	}

	@Override
	public void explicitJoin() {
		super.explicitJoin();
		join();
	}

	@Override
	public void pulse() {
		super.pulse();
		join();
	}

	private void join() {
		Connection sqlConnection = datastoreProvider.getConnection();
		OrientJdbcConnection orientDbConn = (OrientJdbcConnection) sqlConnection;
		ODatabaseDocumentTx database = orientDbConn.getDatabase();
		if ( currentOrientDBTransaction == null && delegate.isActive() ) {
			log.debugf( "begin transaction for database %s", database.getName() );
			database.begin();
			currentOrientDBTransaction = database.getTransaction();
			delegate.getLocalSynchronizations().registerSynchronization( new OrientDBSynchronization() );
		}

	}

	private void success() {
		if ( currentOrientDBTransaction != null ) {
			log.debugf( "commit  transaction N %d for database %s",
					currentOrientDBTransaction.getId(),
					currentOrientDBTransaction.getDatabase().getName() );
			currentOrientDBTransaction.commit();
			currentOrientDBTransaction = null;
		}
	}

	private void failure() {
		if ( currentOrientDBTransaction != null ) {
			log.debugf( "rollback  transaction N %d for database %s",
					currentOrientDBTransaction.getId(),
					currentOrientDBTransaction.getDatabase().getName() );
			currentOrientDBTransaction.rollback();
			currentOrientDBTransaction = null;
		}
	}

	private class OrientDBSynchronization implements Synchronization {

		@Override
		public void beforeCompletion() {
			TransactionStatus status = delegate.getTransactionDriverControl().getStatus();
			if ( status == TransactionStatus.MARKED_ROLLBACK ) {
				failure();
			}
			else {
				success();
			}
		}

		@Override
		public void afterCompletion(int status) {
			if ( currentOrientDBTransaction != null ) {
				if ( status != Status.STATUS_COMMITTED ) {
					failure();
				}
				else {
					success();
				}
			}
		}
	}

}
