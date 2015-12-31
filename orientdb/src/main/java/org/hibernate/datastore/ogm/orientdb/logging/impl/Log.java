/*
 * Copyright (C) 2015 Hibernate.
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA 02110-1301  USA
 */
package org.hibernate.datastore.ogm.orientdb.logging.impl;

import org.hibernate.HibernateException;
import org.hibernate.ogm.model.key.spi.EntityKey;
import org.hibernate.ogm.model.spi.TupleOperation;
import static org.jboss.logging.Logger.Level.DEBUG;
import static org.jboss.logging.Logger.Level.ERROR;
import static org.jboss.logging.Logger.Level.INFO;
import static org.jboss.logging.Logger.Level.WARN;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;

/**
 *
 * @author chernolyassv
 */
@MessageLogger(projectCode = "OGM")
public interface Log extends org.hibernate.ogm.util.impl.Log {

	@Message(id = 1401, value = "Cannot generate sequence %s")
	HibernateException cannotGenerateSequence(String sequenceName);

	@LogMessage(level = DEBUG)
	@Message(id = 1402, value = "An error occured while generating the sequence %s")
	void errorGeneratingSequence(String sequenceName, @Cause Exception e);

	@Message(id = 1403, value = "Constraint violation for entity %s: %s")
	HibernateException constraintViolation(EntityKey entityKey, TupleOperation operation, @Cause Exception cause);

	@LogMessage(level = WARN)
	@Message(id = 1404, value = "Neo4j does not support constraints spanning multiple columns. Unique key %1$s for %2$s on columns %3$s cannot be created")
	void constraintSpanningMultipleColumns(String name, String tableName, String columns);

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
    
}
