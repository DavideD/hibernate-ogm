package com.iampfac.demo.data.jpa;

import static org.fest.assertions.Assertions.assertThat;

import java.util.List;

import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.iampfac.demo.config.ApplicationConfiguration;
import com.iampfac.demo.core.user.User;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = ApplicationConfiguration.class)
@DirtiesContext(classMode = ClassMode.AFTER_CLASS)
public class JpaProxyUserRepositoryIntegrationTest {

	@Autowired
	private JpaProxyUserRepository repository;

	@Test
	public void sampleTestCase() {
		User dave = new User( "Dave", "Mathews" );
		dave = repository.save( dave );

		User carter = new User( "Carter", "Beauford" );
		carter = repository.save( carter );

		List<User> daveResults = repository.byName( dave.getFirstName() );
		assertThat( daveResults ).containsExactly( dave );

		List<User> carterResults = repository.byName( carter.getFirstName() );
		assertThat( carterResults ).containsExactly( carter );
	}

	@After
	public void after() {
		repository.deleteAll();
	}
}
