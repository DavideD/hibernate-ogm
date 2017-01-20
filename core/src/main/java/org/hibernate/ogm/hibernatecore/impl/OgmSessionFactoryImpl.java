/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.hibernatecore.impl;

import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.naming.NamingException;
import javax.naming.Reference;
import javax.naming.StringRefAddr;
import javax.persistence.EntityGraph;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceUnitUtil;
import javax.persistence.Query;
import javax.persistence.SynchronizationType;
import javax.persistence.criteria.CriteriaBuilder;

import org.hibernate.CustomEntityDirtinessStrategy;
import org.hibernate.EntityNameResolver;
import org.hibernate.HibernateException;
import org.hibernate.Interceptor;
import org.hibernate.MappingException;
import org.hibernate.Session;
import org.hibernate.SessionFactoryObserver;
import org.hibernate.StatelessSession;
import org.hibernate.StatelessSessionBuilder;
import org.hibernate.TypeHelper;
import org.hibernate.boot.spi.SessionFactoryOptions;
import org.hibernate.cache.spi.QueryCache;
import org.hibernate.cache.spi.Region;
import org.hibernate.cache.spi.UpdateTimestampsCache;
import org.hibernate.cache.spi.access.RegionAccessStrategy;
import org.hibernate.cfg.Settings;
import org.hibernate.context.spi.CurrentTenantIdentifierResolver;
import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.function.SQLFunctionRegistry;
import org.hibernate.engine.ResultSetMappingDefinition;
import org.hibernate.engine.jdbc.spi.JdbcServices;
import org.hibernate.engine.jdbc.spi.SqlExceptionHelper;
import org.hibernate.engine.profile.FetchProfile;
import org.hibernate.engine.query.spi.QueryPlanCache;
import org.hibernate.engine.spi.CacheImplementor;
import org.hibernate.engine.spi.FilterDefinition;
import org.hibernate.engine.spi.NamedQueryDefinition;
import org.hibernate.engine.spi.NamedSQLQueryDefinition;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.event.spi.EventSource;
import org.hibernate.exception.spi.SQLExceptionConverter;
import org.hibernate.id.IdentifierGenerator;
import org.hibernate.id.factory.IdentifierGeneratorFactory;
import org.hibernate.metadata.ClassMetadata;
import org.hibernate.metadata.CollectionMetadata;
import org.hibernate.metamodel.spi.MetamodelImplementor;
import org.hibernate.ogm.OgmSession;
import org.hibernate.ogm.engine.spi.OgmSessionBuilderImplementor;
import org.hibernate.ogm.engine.spi.OgmSessionFactoryImplementor;
import org.hibernate.ogm.exception.NotSupportedException;
import org.hibernate.persister.collection.CollectionPersister;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.proxy.EntityNotFoundDelegate;
import org.hibernate.query.spi.NamedQueryRepository;
import org.hibernate.service.spi.ServiceRegistryImplementor;
import org.hibernate.stat.spi.StatisticsImplementor;
import org.hibernate.type.Type;
import org.hibernate.type.TypeResolver;

/**
 * @author Emmanuel Bernard &lt;emmanuel@hibernate.org&gt;
 * @author Gunnar Morling
 */
public class OgmSessionFactoryImpl implements OgmSessionFactoryImplementor {

	private final SessionFactoryImplementor delegate;

	public OgmSessionFactoryImpl(SessionFactoryImplementor delegate) {
		this.delegate = delegate;
	}

	@Override
	public TypeResolver getTypeResolver() {
		return delegate.getTypeResolver();
	}

	@Override
	public Map<String, Object> getProperties() {
		return delegate.getProperties();
	}

	@Override
	public EntityPersister getEntityPersister(String entityName) throws MappingException {
		return delegate.getEntityPersister( entityName );
	}

	@Override
	public Map<String, EntityPersister> getEntityPersisters() {
		return delegate.getEntityPersisters();
	}

	@Override
	public CollectionPersister getCollectionPersister(String role) throws MappingException {
		return delegate.getCollectionPersister( role );
	}

	@Override
	public Map<String, CollectionPersister> getCollectionPersisters() {
		return delegate.getCollectionPersisters();
	}

	@Override
	public JdbcServices getJdbcServices() {
		return delegate.getJdbcServices();
	}

	@Override
	public Dialect getDialect() {
		return delegate.getDialect();
	}

	@Override
	public Interceptor getInterceptor() {
		return delegate.getInterceptor();
	}

	@Override
	public QueryPlanCache getQueryPlanCache() {
		return delegate.getQueryPlanCache();
	}

	@Override
	public Type[] getReturnTypes(String queryString) throws HibernateException {
		return delegate.getReturnTypes( queryString );
	}

	@Override
	public String[] getReturnAliases(String queryString) throws HibernateException {
		return delegate.getReturnAliases( queryString );
	}

	@Override
	public String[] getImplementors(String className) throws MappingException {
		return delegate.getImplementors( className );
	}

	@Override
	public String getImportedClassName(String name) {
		return delegate.getImportedClassName( name );
	}

	@Override
	public QueryCache getQueryCache() {
		return delegate.getQueryCache();
	}

	@Override
	public QueryCache getQueryCache(String regionName) throws HibernateException {
		return delegate.getQueryCache( regionName );
	}

	@Override
	public UpdateTimestampsCache getUpdateTimestampsCache() {
		return delegate.getUpdateTimestampsCache();
	}

	@Override
	public StatisticsImplementor getStatisticsImplementor() {
		return delegate.getStatisticsImplementor();
	}

	@Override
	public NamedQueryDefinition getNamedQuery(String queryName) {
		return delegate.getNamedQuery( queryName );
	}

	@Override
	public NamedSQLQueryDefinition getNamedSQLQuery(String queryName) {
		return delegate.getNamedSQLQuery( queryName );
	}

	@Override
	public NamedQueryRepository getNamedQueryRepository() {
		return delegate.getNamedQueryRepository();
	}

	@Override
	public void registerNamedQueryDefinition(String name, NamedQueryDefinition definition) {
		delegate.registerNamedQueryDefinition( name, definition );
	}

	@Override
	public void registerNamedSQLQueryDefinition(String name, NamedSQLQueryDefinition definition) {
		delegate.registerNamedSQLQueryDefinition( name, definition );
	}

	@Override
	public ResultSetMappingDefinition getResultSetMapping(String name) {
		return delegate.getResultSetMapping( name );
	}

	@Override
	public IdentifierGenerator getIdentifierGenerator(String rootEntityName) {
		return delegate.getIdentifierGenerator( rootEntityName );
	}

	@Override
	public Region getSecondLevelCacheRegion(String regionName) {
		return delegate.getSecondLevelCacheRegion( regionName );
	}

	@Override
	public Map getAllSecondLevelCacheRegions() {
		return delegate.getAllSecondLevelCacheRegions();
	}

	@Override
	public SQLExceptionConverter getSQLExceptionConverter() {
		return delegate.getSQLExceptionConverter();
	}

	@Override
	public SqlExceptionHelper getSQLExceptionHelper() {
		return delegate.getSQLExceptionHelper();
	}

	@Override
	public Settings getSettings() {
		return delegate.getSettings();
	}

	@Override
	public OgmSession openTemporarySession() throws HibernateException {
		return new OgmSessionImpl( this, (EventSource) delegate.openTemporarySession() );
	}

	@Override
	public Set<String> getCollectionRolesByEntityParticipant(String entityName) {
		return delegate.getCollectionRolesByEntityParticipant( entityName );
	}

	@Override
	public EntityNotFoundDelegate getEntityNotFoundDelegate() {
		return delegate.getEntityNotFoundDelegate();
	}

	@Override
	public SQLFunctionRegistry getSqlFunctionRegistry() {
		return delegate.getSqlFunctionRegistry();
	}

	@Override
	public FetchProfile getFetchProfile(String name) {
		return delegate.getFetchProfile( name );
	}

	@Override
	public ServiceRegistryImplementor getServiceRegistry() {
		return delegate.getServiceRegistry();
	}

	@Override
	public void addObserver(SessionFactoryObserver observer) {
		delegate.addObserver( observer );
	}

	@Override
	public IdentifierGeneratorFactory getIdentifierGeneratorFactory() {
		return delegate.getIdentifierGeneratorFactory();
	}

	@Override
	public Type getIdentifierType(String className) throws MappingException {
		return delegate.getIdentifierType( className );
	}

	@Override
	public String getIdentifierPropertyName(String className) throws MappingException {
		return delegate.getIdentifierPropertyName( className );
	}

	@Override
	public Type getReferencedPropertyType(String className, String propertyName) throws MappingException {
		return delegate.getReferencedPropertyType( className, propertyName );
	}

	@Override
	public SessionFactoryOptions getSessionFactoryOptions() {
		return delegate.getSessionFactoryOptions();
	}

	@Override
	public OgmSessionBuilderImplementor withOptions() {
		return new OgmSessionBuilderDelegator( delegate.withOptions(), this );
	}

	@Override
	public OgmSession openSession() throws HibernateException {
		final Session session = delegate.openSession();
		return new OgmSessionImpl( this, (EventSource) session );
	}

	@Override
	public OgmSession getCurrentSession() throws HibernateException {
		final Session session = delegate.getCurrentSession();
		return new OgmSessionImpl( this, (EventSource) session );
	}

	@Override
	public StatelessSessionBuilder withStatelessOptions() {
		throw new NotSupportedException( "OGM-18", "Stateless session is not implemented in OGM" );
	}

	@Override
	public StatelessSession openStatelessSession() {
		throw new NotSupportedException( "OGM-18", "Stateless session is not implemented in OGM" );
	}

	@Override
	public StatelessSession openStatelessSession(Connection connection) {
		throw new NotSupportedException( "OGM-18", "Stateless session is not implemented in OGM" );
	}

	@Override
	public ClassMetadata getClassMetadata(Class entityClass) {
		return delegate.getClassMetadata( entityClass );
	}

	@Override
	public ClassMetadata getClassMetadata(String entityName) {
		return delegate.getClassMetadata( entityName );
	}

	@Override
	public CollectionMetadata getCollectionMetadata(String roleName) {
		return delegate.getCollectionMetadata( roleName );
	}

	@Override
	public Map<String, ClassMetadata> getAllClassMetadata() {
		return delegate.getAllClassMetadata();
	}

	@Override
	public Map getAllCollectionMetadata() {
		return delegate.getAllCollectionMetadata();
	}

	@Override
	public StatisticsImplementor getStatistics() {
		return delegate.getStatistics();
	}

	@Override
	public void close() throws HibernateException {
		delegate.close();
	}

	@Override
	public boolean isClosed() {
		return delegate.isClosed();
	}

	@Override
	public CacheImplementor getCache() {
		return delegate.getCache();
	}


	@Override
	public Set getDefinedFilterNames() {
		return delegate.getDefinedFilterNames();
	}

	@Override
	public FilterDefinition getFilterDefinition(String filterName) throws HibernateException {
		return delegate.getFilterDefinition( filterName );
	}

	@Override
	public boolean containsFetchProfileDefinition(String name) {
		return delegate.containsFetchProfileDefinition( name );
	}

	@Override
	public TypeHelper getTypeHelper() {
		return delegate.getTypeHelper();
	}

	@Override
	public CurrentTenantIdentifierResolver getCurrentTenantIdentifierResolver() {
		return delegate.getCurrentTenantIdentifierResolver();
	}

	@Override
	public Region getNaturalIdCacheRegion(String regionName) {
		return delegate.getNaturalIdCacheRegion( regionName );
	}

	@Override
	public CustomEntityDirtinessStrategy getCustomEntityDirtinessStrategy() {
		return delegate.getCustomEntityDirtinessStrategy();
	}

	@Override
	public Reference getReference() throws NamingException {
		//Expect Hibernate Core to use one StringRefAddr based address
		String uuid = String.valueOf( delegate.getReference().get( 0 ).getContent() );
		return new Reference(
				OgmSessionFactoryImpl.class.getName(),
				new StringRefAddr( "uuid", uuid ),
				OgmSessionFactoryObjectFactory.class.getName(),
				null
				);
	}

	@Override
	public Iterable<EntityNameResolver> iterateEntityNameResolvers() {
		return delegate.iterateEntityNameResolvers();
	}

	@Override
	public RegionAccessStrategy getSecondLevelCacheRegionAccessStrategy(String regionName) {
		return delegate.getSecondLevelCacheRegionAccessStrategy( regionName );
	}

	@Override
	public RegionAccessStrategy getNaturalIdCacheRegionAccessStrategy(String regionName) {
		return delegate.getNaturalIdCacheRegionAccessStrategy( regionName );
	}

	@Override
	public EntityPersister locateEntityPersister(Class byClass) {
		return delegate.locateEntityPersister( byClass );
	}

	@Override
	public EntityPersister locateEntityPersister(String byName) {
		return delegate.locateEntityPersister( byName );
	}

	@Override
	public DeserializationResolver getDeserializationResolver() {
		return delegate.getDeserializationResolver();
	}

	@Override
	public String getUuid() {
		return delegate.getUuid();
	}

	@Override
	public EntityManager createEntityManager() {
		return new OgmSessionImpl( this, (EventSource) delegate.createEntityManager() );
	}

	@Override
	public EntityManager createEntityManager(Map map) {
		return new OgmSessionImpl( this, (EventSource) delegate.createEntityManager( map ) );
	}

	@Override
	public EntityManager createEntityManager(SynchronizationType synchronizationType) {
		return new OgmSessionImpl( this, (EventSource) delegate.createEntityManager( synchronizationType ) );
	}

	@Override
	public EntityManager createEntityManager(SynchronizationType synchronizationType, Map map) {
		return new OgmSessionImpl( this, (EventSource) delegate.createEntityManager( synchronizationType, map ) );
	}

	@Override
	public CriteriaBuilder getCriteriaBuilder() {
		return delegate.getCriteriaBuilder();
	}

	@Override
	public boolean isOpen() {
		return delegate.isOpen();
	}

	@Override
	public PersistenceUnitUtil getPersistenceUnitUtil() {
		return delegate.getPersistenceUnitUtil();
	}

	@Override
	public void addNamedQuery(String name, Query query) {
		delegate.addNamedQuery( name, query );
	}

	@Override
	public <T> T unwrap(Class<T> cls) {
		if ( cls != null && cls.isAssignableFrom( getClass() ) ) {
			@SuppressWarnings("unchecked")
			T result = (T) this;
			return result;
		}

		return delegate.unwrap( cls );
	}

	@Override
	public <T> void addNamedEntityGraph(String graphName, EntityGraph<T> entityGraph) {
		throw new IllegalStateException( "Hibernate OGM does not support entity graphs" );
	}

	@Override
	public <T> List<EntityGraph<? super T>> findEntityGraphsByType(Class<T> entityClass) {
		return delegate.findEntityGraphsByType( entityClass );
	}

	@Override
	public String getName() {
		return delegate.getName();
	}

	@Override
	public MetamodelImplementor getMetamodel() {
		return delegate.getMetamodel();
	}

	@Override
	public EntityGraph findEntityGraphByName(String name) {
		return delegate.findEntityGraphByName( name );
	}

	@Override
	public Type resolveParameterBindType(Object bindValue) {
		return delegate.resolveParameterBindType( bindValue );
	}

	@Override
	public Type resolveParameterBindType(Class clazz) {
		return delegate.resolveParameterBindType( clazz );
	}
}
