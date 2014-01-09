/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @authors tag. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */
package org.hibernate.ogm.dialect.infinispan;

import java.util.Map;
import java.util.Set;

import org.hibernate.ogm.datastore.spi.TupleSnapshot;

/**
 * @author Emmanuel Bernard <emmanuel@hibernate.org>
 */
public final class InfinispanTupleSnapshot implements TupleSnapshot {
	private final Map<String, Object> atomicMap;

	public InfinispanTupleSnapshot(Map<String,Object> atomicMap) {
		this.atomicMap = atomicMap;
	}
	@Override
	public Object get(String column) {
		return atomicMap.get( column );
	}

	@Override
	public boolean isEmpty() {
		return atomicMap.isEmpty();
	}

	@Override
	public Set<String> getColumnNames() {
		return atomicMap.keySet();
	}

	public Map<String, Object> getAtomicMap() {
		return atomicMap;
	}
}
