/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.datastore.ogm.orientdb.logging.impl;

import static org.jboss.logging.Logger.Level.DEBUG;
import static org.jboss.logging.Logger.Level.ERROR;
import static org.jboss.logging.Logger.Level.INFO;

import org.hibernate.HibernateException;
import org.hibernate.ogm.model.key.spi.EntityKey;
import org.hibernate.ogm.model.spi.TupleOperation;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;

import com.orientechnologies.orient.core.id.ORecordId;

/**
 * @author Sergey Chernolyas (sergey.chernolyas@gmail.com)
 */
@MessageLogger(projectCode = "OGM")
public interface Log extends org.hibernate.ogm.util.impl.Log {

	@Message(id = 1403, value = "Constraint violation for entity %s: %s")
	HibernateException constraintViolation(EntityKey entityKey, TupleOperation operation, @Cause Exception cause);

	@LogMessage(level = DEBUG)
	@Message(id = 1405, value = "%1$s")
	void logOrientDBQueryEngineMessage(String message);

	@LogMessage(level = ERROR)
	@Message(id = 1406, value = "%1$s")
	void logOrientDBQueryEngineException(String message, @Cause Throwable e);

	@LogMessage(level = INFO)
	@Message(id = 1407, value = "%1$s - %2$s")
	void logOrientDBQueryEngineUserMessage(String marker, String message);

	@Message(id = 1408, value = "Error while cheking transaction status")
	HibernateException exceptionWhileChekingTransactionStatus(@Cause Exception e);

	@Message(id = 1700, value = "Cannot create class %s")
	HibernateException cannotGenerateVertexClass(String className, @Cause Exception cause);

	@Message(id = 1701, value = "Cannot create property %s for class %s")
	HibernateException cannotGenerateProperty(String propertyName, String className, @Cause Exception cause);

	@Message(id = 1702, value = "Cannot create index %s for class %s")
	HibernateException cannotGenerateIndex(String propertyName, String className, @Cause Exception cause);

	@Message(id = 1703, value = "Cannot generate sequence %s")
	HibernateException cannotGenerateSequence(String sequenceName, @Cause Exception cause);

	@Message(id = 1704, value = "Cannot read entity by @rid %s")
	HibernateException cannotReadEntityByRid(ORecordId rid, @Cause Exception cause);

	@Message(id = 1705, value = "Cannot move on ResultSet")
	HibernateException cannotMoveOnResultSet(@Cause Exception cause);

	@Message(id = 1706, value = "Cannot process ResultSet")
	HibernateException cannotProcessResultSet(@Cause Exception cause);

	@Message(id = 1707, value = "Cannot close ResultSet")
	HibernateException cannotCloseResultSet(@Cause Exception cause);

	@Message(id = 1708, value = "Cannot delete row from ResultSet")
	HibernateException cannotDeleteRowFromResultSet(@Cause Exception cause);

	@Message(id = 1709, value = "Cannot execute query %s")
	HibernateException cannotExecuteQuery(String query, @Cause Exception cause);

	@Message(id = 1710, value = "Cannot set value for parameter %d")
	HibernateException cannotSetValueForParameter(Integer paramNum, @Cause Exception cause);
}
