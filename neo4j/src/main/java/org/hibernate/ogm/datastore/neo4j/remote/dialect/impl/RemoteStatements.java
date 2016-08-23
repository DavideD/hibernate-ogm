/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.neo4j.remote.dialect.impl;

import java.util.List;

import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;

/**
 * @author Davide D'Alto
 */
public final class RemoteStatements {

	private RemoteStatements() {
	}

	public static void runAll(Transaction tx, List<Statement> statements) {
		for ( Statement statement : statements ) {
			StatementResult result = tx.run( statement );

			// It will throw an exception in case something went wrong
			result.hasNext();
		}
	}

	public static StatementResult runAllReturnLast(Transaction tx, List<Statement> statements) {
		StatementResult result = null;
		for ( Statement statement : statements ) {
			result = tx.run( statement );
		}
		return result;
	}
}
