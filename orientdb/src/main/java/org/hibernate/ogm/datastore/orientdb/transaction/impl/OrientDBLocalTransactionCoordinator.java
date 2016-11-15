/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.orientdb.transaction.impl;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.tx.OTransaction;
import com.orientechnologies.orient.core.tx.OTransactionNoTx;
import com.orientechnologies.orient.core.tx.OTransactionOptimistic;
import org.hibernate.ogm.datastore.orientdb.impl.OrientDBDatastoreProvider;
import org.hibernate.ogm.datastore.orientdb.logging.impl.Log;
import org.hibernate.ogm.datastore.orientdb.logging.impl.LoggerFactory;
import org.hibernate.ogm.transaction.impl.ForwardingTransactionCoordinator;
import org.hibernate.ogm.transaction.impl.ForwardingTransactionDriver;
import org.hibernate.resource.transaction.TransactionCoordinator;

/**
 * Coordinator for local transactions
 *
 * @author Sergey Chernolyas &lt;sergey.chernolyas@gmail.com&gt;
 */
public class OrientDBLocalTransactionCoordinator extends ForwardingTransactionCoordinator {

	private static Log log = LoggerFactory.getLogger();
	private OrientDBDatastoreProvider datastoreProvider;
	private OTransaction currentOrientDBTransaction;

	/**
	 * Constructor
	 *
	 * @param coordinator transaction coordinator
	 * @param datastoreProvider provider of OrientDB datastore
	 */
	public OrientDBLocalTransactionCoordinator(TransactionCoordinator coordinator, OrientDBDatastoreProvider datastoreProvider) {
		super( coordinator );
		this.datastoreProvider = datastoreProvider;
	}

	@Override
	public TransactionDriver getTransactionDriverControl() {
		TransactionDriver driver = super.getTransactionDriverControl();
		return new OrientDBTransactionDriver( driver );
	}

	private void success() {
		if ( currentOrientDBTransaction != null && currentOrientDBTransaction.isActive() ) {
			log.debugf( "commit transaction (Id: %d) for database %s  is %d.",
					currentOrientDBTransaction.getId(),
					currentOrientDBTransaction.getDatabase().getName() );
			currentOrientDBTransaction.commit( true );
			close();
		}
		else {
			currentOrientDBTransaction = null;
		}
	}

	private void failure() {
		if ( currentOrientDBTransaction != null && currentOrientDBTransaction.isActive() ) {
			log.debugf( "rollback transaction (Id: %d) for database %s  is %d.",
					currentOrientDBTransaction.getId(),
					currentOrientDBTransaction.getDatabase().getName() );
			currentOrientDBTransaction.rollback( true, 1 );
			close();
		}
		else {
			currentOrientDBTransaction = null;
		}
	}

	private void close() {
		try {
			datastoreProvider.closeCurrentDatabase();
		}
		finally {
			currentOrientDBTransaction = null;
		}
	}

	private class OrientDBTransactionDriver extends ForwardingTransactionDriver {

		public OrientDBTransactionDriver(TransactionDriver delegate) {
			super( delegate );
		}

		@Override
		public void begin() {
			ODatabaseDocumentTx database = datastoreProvider.getCurrentDatabase();
			log.debugf( "begin transaction for database %s. Connection's hash code: %s",
					database.getName(), database.hashCode() );
			super.begin();
			/*
			 * if ( database.isClosed() ) { log.debugf( "Database %s closed. Reopen for thread : %s",
			 * database.getName(), Thread.currentThread().getName() ); database.open( "admin", "admin" ); }
			 */
			currentOrientDBTransaction = database.activateOnCurrentThread().begin().getTransaction();
			if ( currentOrientDBTransaction instanceof OTransactionNoTx ) {
				// no active transaction. create it!!
				log.debugf( "no active transactions for database %s . Create new transaction!", database.getName() );
				currentOrientDBTransaction = database.getTransaction();
				currentOrientDBTransaction.setUsingLog( true );
				log.debugf( "Id of new transaction for database %s  is %d.", database.getName(),
						currentOrientDBTransaction.getId() );
			}
			else {
				log.debugf( "Id of current transaction for database %s  is %d. (transaction: %s)", database.getName(),
						currentOrientDBTransaction.getId(), currentOrientDBTransaction );
				OTransactionOptimistic op = (OTransactionOptimistic) currentOrientDBTransaction;
			}
		}

		@Override
		public void commit() {
			try {
				if ( currentOrientDBTransaction != null && currentOrientDBTransaction.isActive() ) {
					log.debugf( "commit transaction N %s for database %s. Transaction acvite? %s",
							String.valueOf( currentOrientDBTransaction.getId() ),
							currentOrientDBTransaction.getDatabase().getName(),
							String.valueOf( currentOrientDBTransaction.isActive() ) );
					super.commit();
					success();

				}

			}
			catch (Exception e) {
				log.error( "Cannot commit transaction!", e );
				try {
					rollback();
				}
				catch (Exception re) {
				}
				throw e;
			}
		}

		@Override
		public void rollback() {
			try {
				if ( currentOrientDBTransaction != null && currentOrientDBTransaction.isActive() ) {
					log.debugf( "rollback  transaction N %s for database %s. Transaction acvite? %s",
							String.valueOf( currentOrientDBTransaction.getId() ),
							currentOrientDBTransaction.getDatabase().getName(),
							String.valueOf( currentOrientDBTransaction.isActive() ) );
					super.rollback();
				}
			}
			catch (Exception e) {
				log.error( "Cannot rollback transaction!", e );
			}
			finally {
				failure();
			}
		}
	}

}
