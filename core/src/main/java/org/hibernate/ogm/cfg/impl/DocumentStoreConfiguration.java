/*
 * Hibernate, Relational Persistence for Idiomatic Java
 *
 * JBoss, Home of Professional Open Source
 * Copyright 2014 Red Hat Inc. and/or its affiliates and other contributors
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
package org.hibernate.ogm.cfg.impl;

import java.util.Map;

import org.hibernate.ogm.cfg.DocumentStoreProperties;
import org.hibernate.ogm.options.generic.document.AssociationStorageType;
import org.hibernate.ogm.util.configurationreader.impl.ConfigurationPropertyReader;

/**
 * Provides access to properties common to different document datastores.
 *
 * @author Gunnar Morling
 */
public abstract class DocumentStoreConfiguration extends CommonStoreConfiguration {

	private static final AssociationStorageType DEFAULT_ASSOCIATION_STORAGE = AssociationStorageType.IN_ENTITY;

	private final AssociationStorageType associationStorage;

	public DocumentStoreConfiguration(Map<?, ?> configurationValues, int defaultPort) {
		super( configurationValues, defaultPort );

		ConfigurationPropertyReader propertyReader = new ConfigurationPropertyReader( configurationValues );
		associationStorage = propertyReader.property( DocumentStoreProperties.ASSOCIATIONS_STORE, AssociationStorageType.class )
				.withDefault( DEFAULT_ASSOCIATION_STORAGE )
				.getValue();
	}

	/**
	 * @see DocumentStoreProperties#ASSOCIATIONS_STORE
	 * @return where to store associations
	 */
	public AssociationStorageType getAssociationStorageStrategy() {
		return associationStorage;
	}
}
