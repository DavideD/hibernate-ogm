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

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = ApplicationConfiguration.class)
@DirtiesContext(classMode = ClassMode.AFTER_CLASS)
public class UserJpaRepositoryIntegrationTest {

	@Autowired
	private UserJpaRepository	repository;

	@Test
	public void sampleTestCase() {
		UserJpaEntity dave = new UserJpaEntity("Dave", "Mathews");
		dave = repository.save(dave);

		UserJpaEntity carter = new UserJpaEntity("Carter", "Beauford");
		carter = repository.save(carter);

		List<UserJpaEntity> daveResults = repository.findByName( dave.getFirstname() );
		assertThat( daveResults ).containsExactly( dave );

		List<UserJpaEntity> carterResults = repository.findByName( carter.getFirstname() );
		assertThat( carterResults ).containsExactly( carter );
	}

	@After
	public void after() {
		repository.deleteAll();
	}
}
