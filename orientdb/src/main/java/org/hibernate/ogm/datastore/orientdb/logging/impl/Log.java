/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.datastore.orientdb.logging.impl;

import org.hibernate.HibernateException;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;

import com.orientechnologies.orient.core.id.ORecordId;
import org.hibernate.ogm.model.key.spi.EntityKey;

/**
 * Description of errors and messages, that can be throw by the module
 *
 * @author Sergey Chernolyas &lt;sergey.chernolyas@gmail.com&gt;
 */
@MessageLogger(projectCode = "OGM")
public interface Log extends org.hibernate.ogm.util.impl.Log {

	@Message(id = 1700, value = "Cannot create class %s")
	HibernateException cannotGenerateClass(String className, @Cause Exception cause);

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

	@Message(id = 1711, value = "Cannot parse embedded class for column %s. JSON %s")
	HibernateException cannotParseEmbeddedClass(String column, String json, @Cause Exception cause);

	@Message(id = 1712, value = "Sequence %s not exists.")
	HibernateException sequenceNotExists(String sequenceName, @Cause Exception cause);

	@Message(id = 1713, value = "Cannot create stored procedure %s .")
	HibernateException cannotCreateStoredProcedure(String storedProcName, @Cause Exception cause);

	@Message(id = 1714, value = "Cannot execute stored procedure %s .")
	HibernateException cannotExecuteStoredProcedure(String storedProcName, @Cause Exception cause);

	@Message(id = 1715, value = "Cannot create connection.")
	HibernateException cannotCreateConnection(@Cause Exception cause);

	@Message(id = 1716, value = "Key: %s. Version %d not actual.")
	HibernateException versionNotActual(EntityKey key, Integer version);

	@Message(id = 1717, value = "Cannot alter database properties")
	HibernateException cannotAlterDatabaseProperties(@Cause Exception cause);

	@Message(id = 1718, value = "Cannot create database %s.")
	HibernateException cannotCreateDatabase(String database, @Cause Exception cause);

	@Message(id = 1719, value = "Query %s is not idempotent.")
	HibernateException queryMustBeIdempotentQuery(String query);
}
