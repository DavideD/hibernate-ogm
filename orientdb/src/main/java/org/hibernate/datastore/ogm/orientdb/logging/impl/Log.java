/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.datastore.ogm.orientdb.logging.impl;

import org.hibernate.HibernateException;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;

import com.orientechnologies.orient.core.id.ORecordId;

/**
 * @author Sergey Chernolyas (sergey.chernolyas@gmail.com)
 */
@MessageLogger(projectCode = "OGM")
public interface Log extends org.hibernate.ogm.util.impl.Log {

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
