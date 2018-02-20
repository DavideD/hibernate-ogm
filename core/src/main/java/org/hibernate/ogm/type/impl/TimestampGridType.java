/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.type.impl;

import java.util.Date;

import org.hibernate.MappingException;
import org.hibernate.engine.spi.Mapping;
import org.hibernate.type.descriptor.java.JdbcTimestampTypeDescriptor;


/**
 * Persists Timestamp as Date in MongoDB:
 *
 * @author Pavel Novikov
 */
public class TimestampGridType extends AbstractGenericBasicType<Date> {

	public static final TimestampGridType INSTANCE = new TimestampGridType();

	public TimestampGridType() {
		super( TimestampGridTypeDescriptor.INSTANCE, JdbcTimestampTypeDescriptor.INSTANCE );
	}

	@Override
	public String getName() {
		return "mongo_timestamp";
	}

	@Override
	public int getColumnSpan(Mapping mapping) throws MappingException {
		return 1;
	}

}
