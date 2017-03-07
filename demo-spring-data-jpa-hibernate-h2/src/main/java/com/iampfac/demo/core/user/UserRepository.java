package com.iampfac.demo.core.user;

import java.util.List;

public interface UserRepository {

	User save(User user);

	List<User> all();

	void deleteAll();
}
