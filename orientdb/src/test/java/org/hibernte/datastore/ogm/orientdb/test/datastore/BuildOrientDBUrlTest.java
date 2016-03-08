/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernte.datastore.ogm.orientdb.test.datastore;

import org.hibernate.boot.registry.classloading.internal.ClassLoaderServiceImpl;
import org.hibernate.datastore.ogm.orientdb.OrientDBProperties;
import org.hibernate.datastore.ogm.orientdb.configuration.impl.OrientDBConfiguration;
import org.hibernate.ogm.cfg.OgmProperties;
import org.hibernate.ogm.options.navigation.impl.OptionsContextImpl;
import org.hibernate.ogm.options.navigation.source.impl.OptionValueSources;
import org.hibernate.ogm.options.spi.OptionsContext;
import org.hibernate.ogm.util.configurationreader.spi.ConfigurationPropertyReader;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Test for the {@link OrientDBConfiguration#buildOrientDBUrl()}
 *
 * @author cristhiank on 3/6/16 (calovi86 at gmail.com).
 */
public class BuildOrientDBUrlTest {

	private static final String NON_EXISTENT_IP = "203.0.113.1";
	private static final String NON_EXISTENT_DB = "/path/to/the/db";
	private Map<String, Object> plocalCfg;
	private Map<String, Object> remoteCfg;
	private Map<String, Object> memoryCfg;
	private OptionsContext globalOptions;
	private ConfigurationPropertyReader reader;

	@Before
	public void setupConfigurationMapAndContexts() {
		plocalCfg = new HashMap<>();
		remoteCfg = new HashMap<>();
		memoryCfg = new HashMap<>();

		plocalCfg.put( OgmProperties.DATABASE, NON_EXISTENT_DB );
		plocalCfg.put( OrientDBProperties.ENGINE, "plocal" );

		remoteCfg.put( OrientDBProperties.ENGINE, "remote" );
		remoteCfg.put( OgmProperties.HOST, NON_EXISTENT_IP );
		remoteCfg.put( OgmProperties.DATABASE, NON_EXISTENT_DB );

		memoryCfg.put( OgmProperties.DATABASE, NON_EXISTENT_DB );
		memoryCfg.put( OrientDBProperties.ENGINE, "memory" );
	}

	@Test
	public void shouldBuildPLocalUrl() {
		reader = new ConfigurationPropertyReader( plocalCfg, new ClassLoaderServiceImpl() );
		globalOptions = OptionsContextImpl.forGlobal( OptionValueSources.getDefaultSources( reader ) );
		OrientDBConfiguration config = new OrientDBConfiguration( reader, globalOptions );
		String EXPECTED_PLOCAL = "plocal:" + NON_EXISTENT_DB;
		assertEquals( config.buildOrientDBUrl(), EXPECTED_PLOCAL );
	}

	@Test
	public void shouldBuildRemoteUrl() {
		reader = new ConfigurationPropertyReader( remoteCfg, new ClassLoaderServiceImpl() );
		globalOptions = OptionsContextImpl.forGlobal( OptionValueSources.getDefaultSources( reader ) );
		OrientDBConfiguration config = new OrientDBConfiguration( reader, globalOptions );
		String EXPECTED_REMOTE = "remote:" + NON_EXISTENT_IP + ":" + OrientDBConfiguration.DEFAULT_BIN_PORT + NON_EXISTENT_DB;
		assertEquals( config.buildOrientDBUrl(), EXPECTED_REMOTE );
	}

	@Test
	public void shouldBuildMemoryUrl() {
		reader = new ConfigurationPropertyReader( memoryCfg, new ClassLoaderServiceImpl() );
		globalOptions = OptionsContextImpl.forGlobal( OptionValueSources.getDefaultSources( reader ) );
		OrientDBConfiguration config = new OrientDBConfiguration( reader, globalOptions );
		String EXPECTED_MEMORY = "memory:" + NON_EXISTENT_DB;
		assertEquals( config.buildOrientDBUrl(), EXPECTED_MEMORY );
	}

}
