/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.ogm.hibernatecore.impl;

import static org.hibernate.cfg.AvailableSettings.JPA_LOCK_TIMEOUT;

import java.util.List;
import java.util.Set;

import javax.persistence.EntityManager;
import javax.persistence.Tuple;

import org.hibernate.AssertionFailure;
import org.hibernate.Criteria;
import org.hibernate.Filter;
import org.hibernate.HibernateException;
import org.hibernate.LockMode;
import org.hibernate.LockOptions;
import org.hibernate.MappingException;
import org.hibernate.NaturalIdLoadAccess;
import org.hibernate.Session;
import org.hibernate.SessionException;
import org.hibernate.SharedSessionBuilder;
import org.hibernate.SimpleNaturalIdLoadAccess;
import org.hibernate.boot.registry.classloading.spi.ClassLoaderService;
import org.hibernate.boot.registry.classloading.spi.ClassLoadingException;
import org.hibernate.engine.ResultSetMappingDefinition;
import org.hibernate.engine.query.spi.HQLQueryPlan;
import org.hibernate.engine.query.spi.sql.NativeSQLQueryConstructorReturn;
import org.hibernate.engine.query.spi.sql.NativeSQLQueryReturn;
import org.hibernate.engine.query.spi.sql.NativeSQLQueryRootReturn;
import org.hibernate.engine.query.spi.sql.NativeSQLQuerySpecification;
import org.hibernate.engine.spi.NamedQueryDefinition;
import org.hibernate.engine.spi.NamedSQLQueryDefinition;
import org.hibernate.engine.spi.QueryParameters;
import org.hibernate.engine.spi.SessionDelegatorBaseImpl;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.event.service.spi.EventListenerGroup;
import org.hibernate.event.service.spi.EventListenerRegistry;
import org.hibernate.event.spi.AutoFlushEvent;
import org.hibernate.event.spi.AutoFlushEventListener;
import org.hibernate.event.spi.EventSource;
import org.hibernate.event.spi.EventType;
import org.hibernate.internal.SessionImpl;
import org.hibernate.jdbc.ReturningWork;
import org.hibernate.jdbc.Work;
import org.hibernate.jpa.QueryHints;
import org.hibernate.jpa.spi.TupleBuilderTransformer;
import org.hibernate.loader.custom.CustomQuery;
import org.hibernate.ogm.OgmSession;
import org.hibernate.ogm.OgmSessionFactory;
import org.hibernate.ogm.datastore.spi.DatastoreConfiguration;
import org.hibernate.ogm.engine.spi.OgmSessionFactoryImplementor;
import org.hibernate.ogm.exception.NotSupportedException;
import org.hibernate.ogm.loader.nativeloader.impl.BackendCustomQuery;
import org.hibernate.ogm.options.navigation.GlobalContext;
import org.hibernate.ogm.util.impl.Log;
import org.hibernate.ogm.util.impl.LoggerFactory;
import org.hibernate.procedure.ProcedureCall;
import org.hibernate.query.ParameterMetadata;
import org.hibernate.query.Query;
import org.hibernate.query.internal.NativeQueryImpl;
import org.hibernate.query.internal.QueryImpl;
import org.hibernate.query.spi.NativeQueryImplementor;
import org.hibernate.query.spi.QueryImplementor;
import org.hibernate.resource.transaction.backend.jta.internal.JtaTransactionCoordinatorImpl;

/**
 * An OGM specific session implementation which delegate most of the work to the underlying Hibernate ORM
 * {@code Session}, except queries which are redirected to the OGM engine.
 *
 * @author Emmanuel Bernard &lt;emmanuel@hibernate.org&gt;
 */
public class OgmSessionImpl extends SessionDelegatorBaseImpl implements OgmSession, EventSource {

	private static final Log log = LoggerFactory.make();

	private LockOptions lockOptions = new LockOptions();

	private final OgmSessionFactoryImpl factory;

	private EventSource delegate;

	public OgmSessionImpl(OgmSessionFactory factory, EventSource delegate) {
		super( delegate );
		this.delegate = delegate;
		this.factory = (OgmSessionFactoryImpl) factory;
	}

	// Overridden methods
	@Override
	public SessionFactoryImplementor getFactory() {
		return factory;
	}

	@Override
	public OgmSessionFactoryImplementor getSessionFactory() {
		return factory;
	}

	@Override
	public Criteria createCriteria(Class persistentClass) {
		// TODO plug the Lucene engine
		throw new NotSupportedException( "OGM-23", "Criteria queries are not supported yet" );
	}

	@Override
	public Criteria createCriteria(Class persistentClass, String alias) {
		// TODO plug the Lucene engine
		throw new NotSupportedException( "OGM-23", "Criteria queries are not supported yet" );
	}

	@Override
	public Criteria createCriteria(String entityName) {
		// TODO plug the Lucene engine
		throw new NotSupportedException( "OGM-23", "Criteria queries are not supported yet" );
	}

	@Override
	public Criteria createCriteria(String entityName, String alias) {
		// TODO plug the Lucene engine
		throw new NotSupportedException( "OGM-23", "Criteria queries are not supported yet" );
	}

	@Override
	public QueryImplementor getNamedQuery(String name) {
		checkOpen();
		checkTransactionSynchStatus();
		delayedAfterCompletion();

		// look as HQL/JPQL first
		final NamedQueryDefinition queryDefinition = factory.getNamedQueryRepository().getNamedQueryDefinition( name );
		if ( queryDefinition != null ) {
			return createQuery( queryDefinition );
		}

		// then as a native query
		final NamedSQLQueryDefinition nativeQueryDefinition = factory.getNamedQueryRepository().getNamedSQLQueryDefinition( name );
		if ( nativeQueryDefinition != null ) {
			return createNativeQuery( nativeQueryDefinition, true );
		}

		throw new IllegalArgumentException( "No query defined for that name [" + name + "]" );
	}

	@Override
	public QueryImplementor createNamedQuery(String name) {
		final QueryImplementor<?> query = buildQueryFromName( name, null );
		query.getParameterMetadata().setOrdinalParametersZeroBased( false );
		return query;
	}

	protected QueryImplementor buildQueryFromName(String name, Class resultType) {
		checkOpen();
		checkTransactionSynchStatus();
		delayedAfterCompletion();

		// todo : apply stored setting at the JPA Query level too

		final NamedQueryDefinition namedQueryDefinition = getFactory().getNamedQueryRepository().getNamedQueryDefinition( name );
		if ( namedQueryDefinition != null ) {
			return createQuery( namedQueryDefinition, resultType );
		}

		final NamedSQLQueryDefinition nativeQueryDefinition = getFactory().getNamedQueryRepository().getNamedSQLQueryDefinition( name );
		if ( nativeQueryDefinition != null ) {
			return (QueryImplementor) createNativeQuery( nativeQueryDefinition, resultType );
		}

		throw new IllegalArgumentException( "No query defined for that name [" + name + "]" ) ;
	}

	protected <T> QueryImplementor<T> createQuery(NamedQueryDefinition namedQueryDefinition, Class<T> resultType) {
		final QueryImplementor<T> query = createQuery( namedQueryDefinition );
		if ( resultType != null ) {
			resultClassChecking( resultType, query );
		}
		return query;
	}

	/*
	 *  Copied from org.hibernate.jpa.spi.AbstractEntityManagerImpl
	 */
	protected void resultClassChecking(Class resultType, NamedSQLQueryDefinition namedQueryDefinition) {
		final SessionFactoryImplementor sfi = (SessionFactoryImplementor) factory.getSessionFactory();

		final NativeSQLQueryReturn[] queryReturns;
		if ( namedQueryDefinition.getQueryReturns() != null ) {
			queryReturns = namedQueryDefinition.getQueryReturns();
		}
		else if ( namedQueryDefinition.getResultSetRef() != null ) {
			final ResultSetMappingDefinition rsMapping = sfi.getResultSetMapping( namedQueryDefinition.getResultSetRef() );
			queryReturns = rsMapping.getQueryReturns();
		}
		else {
			throw new AssertionFailure( "Unsupported named query model. Please report the bug in Hibernate EntityManager" );
		}

		if ( queryReturns.length > 1 ) {
			throw new IllegalArgumentException( "Cannot create TypedQuery for query with more than one return" );
		}

		final NativeSQLQueryReturn nativeSQLQueryReturn = queryReturns[0];

		if ( nativeSQLQueryReturn instanceof NativeSQLQueryRootReturn ) {
			final Class<?> actualReturnedClass;
			final String entityClassName = ( (NativeSQLQueryRootReturn) nativeSQLQueryReturn ).getReturnEntityName();
			try {
				actualReturnedClass = sfi.getServiceRegistry().getService( ClassLoaderService.class ).classForName( entityClassName );
			}
			catch ( ClassLoadingException e ) {
				throw new AssertionFailure(
						"Unable to load class [" + entityClassName + "] declared on named native query [" +
								namedQueryDefinition.getName() + "]"
				);
			}
			if ( !resultType.isAssignableFrom( actualReturnedClass ) ) {
				throw buildIncompatibleException( resultType, actualReturnedClass );
			}
		}
		else if ( nativeSQLQueryReturn instanceof NativeSQLQueryConstructorReturn ) {
			final NativeSQLQueryConstructorReturn ctorRtn = (NativeSQLQueryConstructorReturn) nativeSQLQueryReturn;
			if ( !resultType.isAssignableFrom( ctorRtn.getTargetClass() ) ) {
				throw buildIncompatibleException( resultType, ctorRtn.getTargetClass() );
			}
		}
		else {
			//TODO support other NativeSQLQueryReturn type. For now let it go.
		}
	}

	/*
	 *  Copied from org.hibernate.jpa.spi.AbstractEntityManagerImpl
	 */
	private void resultClassChecking(Class resultClass, org.hibernate.Query hqlQuery) {
		// make sure the query is a select -> HHH-7192
		final SessionImplementor session = unwrap( SessionImplementor.class );
		final HQLQueryPlan queryPlan = session.getFactory().getQueryPlanCache()
				.getHQLQueryPlan( hqlQuery.getQueryString(), false, session.getLoadQueryInfluencers().getEnabledFilters() );
		if ( queryPlan.getTranslators()[0].isManipulationStatement() ) {
			throw new IllegalArgumentException( "Update/delete queries cannot be typed" );
		}

		// do some return type validation checking
		if ( Object[].class.equals( resultClass ) ) {
			// no validation needed
		}
		else if ( Tuple.class.equals( resultClass ) ) {
			TupleBuilderTransformer tupleTransformer = new TupleBuilderTransformer( hqlQuery );
			hqlQuery.setResultTransformer( tupleTransformer );
		}
		else {
			final Class dynamicInstantiationClass = queryPlan.getDynamicInstantiationResultType();
			if ( dynamicInstantiationClass != null ) {
				if ( !resultClass.isAssignableFrom( dynamicInstantiationClass ) ) {
					throw new IllegalArgumentException( "Mismatch in requested result type [" + resultClass.getName() + "] and actual result type ["
							+ dynamicInstantiationClass.getName() + "]" );
				}
			}
			else if ( hqlQuery.getReturnTypes().length == 1 ) {
				// if we have only a single return expression, its java type should match with the requested type
				if ( !resultClass.isAssignableFrom( hqlQuery.getReturnTypes()[0].getReturnedClass() ) ) {
					throw new IllegalArgumentException( "Type specified for TypedQuery [" + resultClass.getName()
							+ "] is incompatible with query return type [" + hqlQuery.getReturnTypes()[0].getReturnedClass() + "]" );
				}
			}
			else {
				throw new IllegalArgumentException( "Cannot create TypedQuery for query with more than one return using requested result type ["
						+ resultClass.getName() + "]" );
			}
		}
	}

	private IllegalArgumentException buildIncompatibleException(Class<?> resultClass, Class<?> actualResultClass) {
		return new IllegalArgumentException(
				"Type specified for TypedQuery [" + resultClass.getName() +
						"] is incompatible with query return type [" + actualResultClass + "]"
		);
	}

	protected QueryImplementor createQuery(NamedQueryDefinition queryDefinition) {
		String queryString = queryDefinition.getQueryString();
		final QueryImplementor query = new QueryImpl(
				this,
				getQueryPlan( queryString, false ).getParameterMetadata(),
				queryString );
		query.setHibernateFlushMode( queryDefinition.getFlushMode() );
		query.setComment( queryDefinition.getComment() != null ? queryDefinition.getComment() : queryDefinition.getName() );
		if ( queryDefinition.getLockOptions() != null ) {
			query.setLockOptions( queryDefinition.getLockOptions() );
		}

		initQueryFromNamedDefinition( query, queryDefinition );
		// applyQuerySettingsAndHints( query );

		return query;
	}

	protected HQLQueryPlan getQueryPlan(String query, boolean shallow) throws HibernateException {
		return getFactory().getQueryPlanCache().getHQLQueryPlan( query, shallow, getLoadQueryInfluencers().getEnabledFilters() );
	}

	@Override
	public NativeQueryImplementor createNativeQuery(String sqlString) {
		final NativeQueryImpl query = (NativeQueryImpl) getNativeQueryImplementor( sqlString, true );
		return query;
	}

	@SuppressWarnings({ "WeakerAccess", "unchecked" })
	protected NativeQueryImplementor createNativeQuery(NamedSQLQueryDefinition queryDefinition, Class resultType) {
		if ( resultType != null ) {
			resultClassChecking( resultType, queryDefinition );
		}

		final NativeQueryImpl query = new NativeQueryImpl(
				queryDefinition,
				this,
				factory.getQueryPlanCache().getSQLParameterMetadata( queryDefinition.getQueryString(), true ) );
		query.setHibernateFlushMode( queryDefinition.getFlushMode() );
		query.setComment( queryDefinition.getComment() != null ? queryDefinition.getComment() : queryDefinition.getName() );
		if ( queryDefinition.getLockOptions() != null ) {
			query.setLockOptions( queryDefinition.getLockOptions() );
		}

		initQueryFromNamedDefinition( query, queryDefinition );
		applyQuerySettingsAndHints( query );

		return query;
	}


	@Override
	public NativeQueryImplementor createNativeQuery(String sqlString, Class resultClass) {
		checkOpen();
		checkTransactionSynchStatus();
		delayedAfterCompletion();

		try {
			NativeQueryImplementor query = createNativeQuery( sqlString );
			query.addEntity( "alias1", resultClass.getName(), LockMode.READ );
			return query;
		}
		catch (RuntimeException he) {
			throw he;
		}
	}

	@Override
	public NativeQueryImplementor createNativeQuery(String sqlString, String resultSetMapping) {
		checkOpen();
		checkTransactionSynchStatus();
		delayedAfterCompletion();

		try {
			NativeQueryImplementor query = createNativeQuery( sqlString );
			query.setResultSetMapping( resultSetMapping );
			return query;
		}
		catch (RuntimeException he) {
			throw he;
		}
	}

	@Override
	public NativeQueryImplementor getNamedNativeQuery(String name) {
		checkOpen();
		checkTransactionSynchStatus();
		delayedAfterCompletion();

		final NamedSQLQueryDefinition nativeQueryDefinition = factory.getNamedQueryRepository().getNamedSQLQueryDefinition( name );
		if ( nativeQueryDefinition != null ) {
			return createNativeQuery( nativeQueryDefinition, true );
		}

		throw new IllegalArgumentException( "No query defined for that name [" + name + "]" );
	}

	@Override
	public NativeQueryImplementor createSQLQuery(String queryString) {
		return getNativeQueryImplementor( queryString, true );
	}

	protected NativeQueryImplementor getNativeQueryImplementor(
			String queryString,
			boolean isOrdinalParameterZeroBased) {
		checkOpen();
		checkTransactionSynchStatus();
		delayedAfterCompletion();

		try {
			NativeQueryImpl query = new NativeQueryImpl(
					queryString,
					false,
					this,
					getFactory().getQueryPlanCache().getSQLParameterMetadata( queryString, isOrdinalParameterZeroBased ) );
			query.setComment( "dynamic native SQL query" );
			return query;
		}
		catch (RuntimeException he) {
			throw he;
		}
	}

	private <T> NativeQueryImplementor<T> createNativeQuery(NamedSQLQueryDefinition queryDefinition, boolean isOrdinalParameterZeroBased) {
		final ParameterMetadata parameterMetadata = factory.getQueryPlanCache().getSQLParameterMetadata(
				queryDefinition.getQueryString(),
				isOrdinalParameterZeroBased );
		return getNativeQueryImplementor( queryDefinition, parameterMetadata );
	}

	private <T> NativeQueryImplementor<T> getNativeQueryImplementor(
			NamedSQLQueryDefinition queryDefinition,
			ParameterMetadata parameterMetadata) {
		final NativeQueryImpl<T> query = new NativeQueryImpl(
				queryDefinition,
				this,
				parameterMetadata );
		query.setComment( queryDefinition.getComment() != null ? queryDefinition.getComment() : queryDefinition.getName() );

		initQueryFromNamedDefinition( query, queryDefinition );
		applyQuerySettingsAndHints( query );

		return query;
	}

	protected <T> void initQueryFromNamedDefinition(Query<T> query, NamedQueryDefinition nqd) {
		// todo : cacheable and readonly should be Boolean rather than boolean...
		query.setCacheable( nqd.isCacheable() );
		query.setCacheRegion( nqd.getCacheRegion() );
		query.setReadOnly( nqd.isReadOnly() );

		if ( nqd.getTimeout() != null ) {
			query.setTimeout( nqd.getTimeout() );
		}
		if ( nqd.getFetchSize() != null ) {
			query.setFetchSize( nqd.getFetchSize() );
		}
		if ( nqd.getCacheMode() != null ) {
			query.setCacheMode( nqd.getCacheMode() );
		}
		if ( nqd.getComment() != null ) {
			query.setComment( nqd.getComment() );
		}
		if ( nqd.getFirstResult() != null ) {
			query.setFirstResult( nqd.getFirstResult() );
		}
		if ( nqd.getMaxResults() != null ) {
			query.setMaxResults( nqd.getMaxResults() );
		}
		if ( nqd.getFlushMode() != null ) {
			query.setHibernateFlushMode( nqd.getFlushMode() );
		}
	}

	protected <T> void applyQuerySettingsAndHints(Query<T> query) {
		if ( lockOptions.getLockMode() != LockMode.NONE ) {
			query.setLockMode( getLockMode( lockOptions.getLockMode() ) );
		}
		Object queryTimeout;
		if ( ( queryTimeout = getProperties().get( QueryHints.SPEC_HINT_TIMEOUT ) ) != null ) {
			query.setHint( QueryHints.SPEC_HINT_TIMEOUT, queryTimeout );
		}
		Object lockTimeout;
		if ( ( lockTimeout = getProperties().get( JPA_LOCK_TIMEOUT ) ) != null ) {
			query.setHint( JPA_LOCK_TIMEOUT, lockTimeout );
		}
	}

	@Override
	public QueryImplementor createFilter(Object collection, String queryString) throws HibernateException {
		// TODO plug the Lucene engine
		throw new NotSupportedException( "OGM-24", "filters are not supported yet" );
	}

	@Override
	public Filter enableFilter(String filterName) {
		throw new NotSupportedException( "OGM-25", "filters are not supported yet" );
	}

	@Override
	public void disableFilter(String filterName) {
		throw new NotSupportedException( "OGM-25", "filters are not supported yet" );
	}

	@Override
	public void doWork(Work work) throws HibernateException {
		throw new IllegalStateException( "Hibernate OGM does not support SQL Connections hence no Work" );
	}

	@Override
	public <T> T doReturningWork(ReturningWork<T> work) throws HibernateException {
		return delegate.doReturningWork( work );
	}

	@Override
	public ProcedureCall getNamedProcedureCall(String name) {
		throw new NotSupportedException( "OGM-359", "Stored procedures are not supported yet" );
	}

	@Override
	public ProcedureCall createStoredProcedureCall(String procedureName) {
		throw new NotSupportedException( "OGM-359", "Stored procedures are not supported yet" );
	}

	@Override
	public ProcedureCall createStoredProcedureCall(String procedureName, Class... resultClasses) {
		throw new NotSupportedException( "OGM-359", "Stored procedures are not supported yet" );
	}

	@Override
	public ProcedureCall createStoredProcedureCall(String procedureName, String... resultSetMappings) {
		throw new NotSupportedException( "OGM-359", "Stored procedures are not supported yet" );
	}

	@Override
	public List<?> listCustomQuery(CustomQuery customQuery, QueryParameters queryParameters) throws HibernateException {
		errorIfClosed();
		checkTransactionSynchStatus();

		if ( log.isTraceEnabled() ) {
			log.tracev( "NoSQL query: {0}", customQuery.getSQL() );
		}

		BackendCustomLoader loader = new BackendCustomLoader( (BackendCustomQuery<?>) customQuery, getFactory() );
		autoFlushIfRequired( loader.getQuerySpaces() );

		return loader.list( getDelegate(), queryParameters );
	}

	/**
	 * detect in-memory changes, determine if the changes are to tables named in the query and, if so, complete
	 * execution the flush
	 * <p>
	 * NOTE: Copied as-is from {@link SessionImpl}. We need it here as
	 * {@link #listCustomQuery(CustomQuery, QueryParameters)} needs to be customized (which makes use of auto flushes)
	 * to work with our custom loaders.
	 */
	private boolean autoFlushIfRequired(Set<String> querySpaces) throws HibernateException {
		if ( !isTransactionInProgress() ) {
			// do not auto-flush while outside a transaction
			return false;
		}
		AutoFlushEvent event = new AutoFlushEvent( querySpaces, getDelegate() );
		for ( AutoFlushEventListener listener : listeners( EventType.AUTO_FLUSH ) ) {
			listener.onAutoFlush( event );
		}
		return event.isFlushRequired();
	}

	private <T> Iterable<T> listeners(EventType<T> type) {
		return eventListenerGroup( type ).listeners();
	}

	private <T> EventListenerGroup<T> eventListenerGroup(EventType<T> type) {
		return factory.getServiceRegistry().getService( EventListenerRegistry.class ).getEventListenerGroup( type );
	}

	@Override
	public List<?> list(NativeSQLQuerySpecification spec, QueryParameters queryParameters) throws HibernateException {
		return listCustomQuery(
				factory.getQueryPlanCache().getNativeSQLQueryPlan( spec ).getCustomQuery(),
				queryParameters );
	}

	@Override
	public NativeQueryImplementor getNamedSQLQuery(String name) {
		final NativeQueryImpl nativeQuery = (NativeQueryImpl) getNamedNativeQuery( name );
		nativeQuery.setZeroBasedParametersIndex( true );
		return nativeQuery;
	}

	private NamedSQLQueryDefinition findNamedNativeQuery(String queryName) {
		NamedSQLQueryDefinition nsqlqd = factory.getNamedSQLQuery( queryName );
		if ( nsqlqd == null ) {
			throw new MappingException( "Named native query not found: " + queryName );
		}
		return nsqlqd;
	}

	@Override
	public SharedSessionBuilder sessionWithOptions() {
		return new OgmSharedSessionBuilderDelegator( delegate.sessionWithOptions(), factory );
	}

	// Copied from org.hibernate.internal.AbstractSessionImpl.errorIfClosed() to mimic same behaviour
	protected void errorIfClosed() {
		if ( delegate.isClosed() ) {
			throw new SessionException( "Session is closed!" );
		}
	}

	// Copied from org.hibernate.internal.SessionImpl.checkTransactionSynchStatus() to mimic same behaviour
	private void checkTransactionSynchStatus() {
		pulseTransactionCoordinator();
		delayedAfterCompletion();
	}

	// Copied from org.hibernate.internal.SessionImpl.pulseTransactionCoordinator() to mimic same behaviour
	private void pulseTransactionCoordinator() {
		if ( !isClosed() ) {
			delegate.getTransactionCoordinator().pulse();
		}
	}

	// Copied from org.hibernate.internal.SessionImpl.delayedAfterCompletion() to mimic same behaviour
	private void delayedAfterCompletion() {
		if ( delegate.getTransactionCoordinator() instanceof JtaTransactionCoordinatorImpl ) {
			( (JtaTransactionCoordinatorImpl) delegate.getTransactionCoordinator() ).getSynchronizationCallbackCoordinator().processAnyDelayedAfterCompletion();
		}
	}

	public <G extends GlobalContext<?, ?>, D extends DatastoreConfiguration<G>> G configureDatastore(Class<D> datastoreType) {
		throw new UnsupportedOperationException( "OGM-343 Session specific options are not currently supported" );
	}

	/**
	 * Returns the underlying ORM session to which most work is delegated.
	 *
	 * @return the underlying session
	 */
	public EventSource getDelegate() {
		return delegate;
	}

	@Override
	public NaturalIdLoadAccess byNaturalId(Class entityClass) {
		throw new UnsupportedOperationException( "OGM-589 - Natural id look-ups are not yet supported" );
	}

	@Override
	public NaturalIdLoadAccess byNaturalId(String entityName) {
		throw new UnsupportedOperationException( "OGM-589 - Natural id look-ups are not yet supported" );
	}

	@Override
	public SimpleNaturalIdLoadAccess bySimpleNaturalId(Class entityClass) {
		throw new UnsupportedOperationException( "OGM-589 - Natural id look-ups are not yet supported" );
	}

	@Override
	public SimpleNaturalIdLoadAccess bySimpleNaturalId(String entityName) {
		throw new UnsupportedOperationException( "OGM-589 - Natural id look-ups are not yet supported" );
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T> T unwrap(Class<T> clazz) {
		checkOpen();

		if ( Session.class.isAssignableFrom( clazz ) ) {
			return (T) this;
		}
		if ( SessionImplementor.class.isAssignableFrom( clazz ) ) {
			return (T) this;
		}
		if ( SharedSessionContractImplementor.class.isAssignableFrom( clazz ) ) {
			return (T) this;
		}
		if ( EntityManager.class.isAssignableFrom( clazz ) ) {
			return (T) this;
		}

		return super.unwrap( clazz );
	}
}
