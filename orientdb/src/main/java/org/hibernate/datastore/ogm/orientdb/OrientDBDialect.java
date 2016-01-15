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
package org.hibernate.datastore.ogm.orientdb;

import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.hibernate.datastore.ogm.orientdb.dialect.impl.OrientDBAssociationQueries;
import org.hibernate.datastore.ogm.orientdb.dialect.impl.OrientDBEntityQueries;
import org.hibernate.datastore.ogm.orientdb.dialect.impl.OrientDBTupleSnapshot;
import org.hibernate.datastore.ogm.orientdb.impl.OrientDBDatastoreProvider;
import org.hibernate.datastore.ogm.orientdb.logging.impl.Log;
import org.hibernate.datastore.ogm.orientdb.logging.impl.LoggerFactory;
import org.hibernate.datastore.ogm.orientdb.query.impl.OrientDBParameterMetadataBuilder;
import org.hibernate.datastore.ogm.orientdb.type.spi.ORecordIdGridType;
import org.hibernate.engine.spi.QueryParameters;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.ogm.dialect.multiget.spi.MultigetGridDialect;
import org.hibernate.ogm.dialect.query.spi.BackendQuery;
import org.hibernate.ogm.dialect.query.spi.ClosableIterator;
import org.hibernate.ogm.dialect.query.spi.ParameterMetadataBuilder;
import org.hibernate.ogm.dialect.query.spi.QueryableGridDialect;
import org.hibernate.ogm.dialect.spi.AssociationContext;
import org.hibernate.ogm.dialect.spi.AssociationTypeContext;
import org.hibernate.ogm.dialect.spi.BaseGridDialect;
import org.hibernate.ogm.dialect.spi.ModelConsumer;
import org.hibernate.ogm.dialect.spi.NextValueRequest;
import org.hibernate.ogm.dialect.spi.SessionFactoryLifecycleAwareDialect;
import org.hibernate.ogm.dialect.spi.TupleAlreadyExistsException;
import org.hibernate.ogm.dialect.spi.TupleContext;
import org.hibernate.ogm.model.key.spi.AssociationKey;
import org.hibernate.ogm.model.key.spi.AssociationKeyMetadata;
import org.hibernate.ogm.model.key.spi.EntityKey;
import org.hibernate.ogm.model.key.spi.EntityKeyMetadata;
import org.hibernate.ogm.model.spi.Association;
import org.hibernate.ogm.model.spi.Tuple;
import org.hibernate.ogm.persister.impl.OgmCollectionPersister;
import org.hibernate.ogm.persister.impl.OgmEntityPersister;
import org.hibernate.ogm.type.spi.GridType;
import org.hibernate.persister.collection.CollectionPersister;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.service.spi.ServiceRegistryAwareService;
import org.hibernate.service.spi.ServiceRegistryImplementor;
import org.hibernate.type.Type;

/**
 *
 * @author Sergey Chernolyas (sergey.chernolyas@gmail.com)
 */
public class OrientDBDialect extends BaseGridDialect implements MultigetGridDialect, QueryableGridDialect<String>,
        ServiceRegistryAwareService, SessionFactoryLifecycleAwareDialect {

    private static final Log log = LoggerFactory.getLogger();

    private ServiceRegistryImplementor serviceRegistry;
    private OrientDBDatastoreProvider provider;
    private Map<AssociationKeyMetadata, OrientDBAssociationQueries> associationQueries;
    private Map<EntityKeyMetadata, OrientDBEntityQueries> entityQueries;

    public OrientDBDialect(OrientDBDatastoreProvider provider) {
        this.provider = provider;
    }

    @Override
    public Tuple getTuple(EntityKey key, TupleContext tupleContext) {
        log.info("EntityKey:" + key + ";");
        for (int i = 0; i < key.getColumnNames().length; i++) {
            String columnName = key.getColumnNames()[i];
            Object columnValue = key.getColumnValues()[i];
            log.info("EntityKey: columnName: " + columnName + ";columnValue: " + columnValue + " (class:" + columnValue.getClass().getName() + ");");
        }
        try {
            Map<String, Object> dbValuesMap = entityQueries.get(key.getMetadata()).findEntity(provider.getConnection(), key.getColumnValues());
            return new Tuple(new OrientDBTupleSnapshot(dbValuesMap, tupleContext.getAllAssociatedEntityKeyMetadata(),
                    tupleContext.getAllRoles(),
                    key.getMetadata()));
        } catch (SQLException e) {
            log.error("Can not find entity", e);
        }
        return null;        
    }

    @Override
    public Tuple createTuple(EntityKey key, TupleContext tupleContext) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void insertOrUpdateTuple(EntityKey key, Tuple tuple, TupleContext tupleContext) throws TupleAlreadyExistsException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void removeTuple(EntityKey key, TupleContext tupleContext) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Association getAssociation(AssociationKey key, AssociationContext associationContext) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Association createAssociation(AssociationKey key, AssociationContext associationContext) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void insertOrUpdateAssociation(AssociationKey key, Association association, AssociationContext associationContext) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void removeAssociation(AssociationKey key, AssociationContext associationContext) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public boolean isStoredInEntityStructure(AssociationKeyMetadata associationKeyMetadata, AssociationTypeContext associationTypeContext) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Number nextValue(NextValueRequest request) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void forEachTuple(ModelConsumer consumer, EntityKeyMetadata... entityKeyMetadatas) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public List<Tuple> getTuples(EntityKey[] keys, TupleContext tupleContext) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public ClosableIterator<Tuple> executeBackendQuery(BackendQuery<String> query, QueryParameters queryParameters) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public ParameterMetadataBuilder getParameterMetadataBuilder() {
        return new OrientDBParameterMetadataBuilder();
    }

    @Override
    public String parseNativeQuery(String nativeQuery) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void injectServices(ServiceRegistryImplementor serviceRegistry) {
        this.serviceRegistry = serviceRegistry;
    }

    @Override
    public void sessionFactoryCreated(SessionFactoryImplementor sessionFactoryImplementor) {

        this.associationQueries = initializeAssociationQueries(sessionFactoryImplementor);
        this.entityQueries = initializeEntityQueries(sessionFactoryImplementor, associationQueries);
    }

    private Map<EntityKeyMetadata, OrientDBEntityQueries> initializeEntityQueries(SessionFactoryImplementor sessionFactoryImplementor,
            Map<AssociationKeyMetadata, OrientDBAssociationQueries> associationQueries) {
        Map<EntityKeyMetadata, OrientDBEntityQueries> entityQueries = initializeEntityQueries(sessionFactoryImplementor);
        for (AssociationKeyMetadata associationKeyMetadata : associationQueries.keySet()) {
            EntityKeyMetadata entityKeyMetadata = associationKeyMetadata.getAssociatedEntityKeyMetadata().getEntityKeyMetadata();
            if (!entityQueries.containsKey(entityKeyMetadata)) {
                // Embeddables metadata
                entityQueries.put(entityKeyMetadata, new OrientDBEntityQueries(entityKeyMetadata));
            }
        }
        return entityQueries;
    }

    private Map<EntityKeyMetadata, OrientDBEntityQueries> initializeEntityQueries(SessionFactoryImplementor sessionFactoryImplementor) {
        Map<EntityKeyMetadata, OrientDBEntityQueries> queryMap = new HashMap<EntityKeyMetadata, OrientDBEntityQueries>();
        Collection<EntityPersister> entityPersisters = sessionFactoryImplementor.getEntityPersisters().values();
        for (EntityPersister entityPersister : entityPersisters) {
            if (entityPersister instanceof OgmEntityPersister) {
                OgmEntityPersister ogmEntityPersister = (OgmEntityPersister) entityPersister;
                queryMap.put(ogmEntityPersister.getEntityKeyMetadata(), new OrientDBEntityQueries(ogmEntityPersister.getEntityKeyMetadata()));
            }
        }
        return queryMap;
    }

    private Map<AssociationKeyMetadata, OrientDBAssociationQueries> initializeAssociationQueries(SessionFactoryImplementor sessionFactoryImplementor) {
        Map<AssociationKeyMetadata, OrientDBAssociationQueries> queryMap = new HashMap<AssociationKeyMetadata, OrientDBAssociationQueries>();
        Collection<CollectionPersister> collectionPersisters = sessionFactoryImplementor.getCollectionPersisters().values();
        for (CollectionPersister collectionPersister : collectionPersisters) {
            if (collectionPersister instanceof OgmCollectionPersister) {
                OgmCollectionPersister ogmCollectionPersister = (OgmCollectionPersister) collectionPersister;
                EntityKeyMetadata ownerEntityKeyMetadata = ((OgmEntityPersister) (ogmCollectionPersister.getOwnerEntityPersister())).getEntityKeyMetadata();
                AssociationKeyMetadata associationKeyMetadata = ogmCollectionPersister.getAssociationKeyMetadata();
                queryMap.put(associationKeyMetadata, new OrientDBAssociationQueries(ownerEntityKeyMetadata, associationKeyMetadata));
            }
        }
        return queryMap;
    }

    @Override
    public GridType overrideType(Type type) {
        log.info("overrideType:" + type.getName() + ";" + type.getReturnedClass());
        GridType gridType = null;
        if (type.getName().equals("com.orientechnologies.orient.core.id.ORecordId")) {
            gridType = ORecordIdGridType.INSTANCE;
        } else {
            gridType = super.overrideType(type); //To change body of generated methods, choose Tools | Templates.
        }
        return gridType;
    }

}
