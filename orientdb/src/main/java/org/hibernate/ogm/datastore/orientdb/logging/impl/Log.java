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

/**
 * Description of errors and messages, that can be throw by the module
 *
 * @author Sergey Chernolyas &lt;sergey.chernolyas@gmail.com&gt;
 */
@MessageLogger(projectCode = "OGM")
public interface Log extends org.hibernate.ogm.util.impl.Log {

	@Message(id = 1700, value = "Cannot use field %s in  class %s! It is unsupported field!")
	HibernateException cannotUseInEntityUnsupportedSystemField(String fieldName, String className);

	@Message(id = 1701, value = "Cannot execute query %s!")
	HibernateException cannotExecuteQuery(String propertyQuery, @Cause Exception cause);

	@Message(id = 1702, value = "Cannot use unsupported type %s!")
	HibernateException cannotUseUnsupportedType(Class type);

	@Message(id = 1703, value = "Cannot create database %s !")
	HibernateException cannotCreateDatabase(String database, @Cause Exception cause);
}
