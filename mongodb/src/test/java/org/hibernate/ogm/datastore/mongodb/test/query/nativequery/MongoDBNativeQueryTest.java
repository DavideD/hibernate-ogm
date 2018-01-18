package org.hibernate.ogm.datastore.mongodb.test.query.nativequery;

import static org.fest.assertions.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EntityManager;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.Table;

import org.bson.types.ObjectId;
import org.hibernate.ogm.utils.jpa.OgmJpaTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MongoDBNativeQueryTest extends OgmJpaTestCase {

	private EntityManager em;
	private Service parasitology = new Service( "Parasitology" );
	private Service serology = new Service( "Serology" );
	private Service virology = new Service( "Microbiology and Virology" );

	private Lab labA = new Lab( "Alpha" );
	private Lab labB = new Lab( "Beta" );

	@Before
	public void init() throws Exception {
		labA.addService( parasitology );
		labA.addService( serology );
		labA.addService( virology );

		// Prepare test data
		em = createEntityManager();
		begin();
		em = persist( labA, labB, serology, virology, parasitology );
		commit();
		em.close();
		em = createEntityManager();
	}

	@After
	public void tearDown() throws Exception {
		begin();
		delete( labA, labB, serology, virology, parasitology );
		commit();
		close( em );
	}

	@Test
	public void test() {
		String query = "db.Lab.aggregate([{'$project': { 'numberOfServices': { '$size': '$listOfServices'}}}, {'$match': {'_id': ObjectId('" + labA.getId() + "')}}, {'$project':{ '_id': 0}}])";

		Integer size = (Integer) em.createNativeQuery( query ).getSingleResult();

		assertThat( size ).isEqualTo( 3 );
	}

	@Override
	protected Class<?>[] getAnnotatedClasses() {
		return new Class<?>[]{ Service.class, Lab.class };
	}

	private void close(EntityManager em) {
		em.clear();
		em.close();
	}

	private EntityManager persist(Object... entities) {
		for ( Object object : entities ) {
			em.persist( object );
		}
		return em;
	}

	private EntityManager delete(Object... entities) {
		for ( Object object : entities ) {
			Object entity = em.merge( object );
			em.remove( entity );
		}
		return em;
	}

	private void begin() throws Exception {
		em.getTransaction().begin();
	}

	private void commit() throws Exception {
		em.getTransaction().commit();
	}

	private void rollback() throws Exception {
		em.getTransaction().rollback();
	}

	private EntityManager createEntityManager() {
		return getFactory().createEntityManager();
	}

	@Entity(name = "Service")
	@Table(name = "Service")
	public static class Service {

		@Id
		@GeneratedValue(strategy = GenerationType.IDENTITY)
		@Column(name = "SERVICE_ID", unique = true, nullable = false)
		public ObjectId id;

		private String name;

		@ManyToOne
		public Lab lab;

		public Service() {
		}

		public Service(String name) {
			this.name = name;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public Lab getLab() {
			return lab;
		}

		public void setLab(Lab lab) {
			this.lab = lab;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ( ( name == null ) ? 0 : name.hashCode() );
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if ( this == obj ) {
				return true;
			}
			if ( obj == null ) {
				return false;
			}
			if ( getClass() != obj.getClass() ) {
				return false;
			}
			Service other = (Service) obj;
			if ( name == null ) {
				if ( other.name != null ) {
					return false;
				}
			}
			else if ( !name.equals( other.name ) ) {
				return false;
			}
			return true;
		}

		@Override
		public String toString() {
			return "Service[" + id + ", " + name + "]";
		}
	}

	@Entity(name = "Lab")
	@Table(name = "Lab")
	public static class Lab {

		@Id
		@GeneratedValue(strategy = GenerationType.IDENTITY)
		@Column(name = "LAB_ID", unique = true, nullable = false)
		public ObjectId id;

		private String name;

		@OneToMany(mappedBy = "lab")
		public List<Service> listOfServices;

		public Integer numberOfServices;

		public Lab() {
		}

		public Lab(String name) {
			this.name = name;
		}

		public void addService(Service service) {
			service.setLab( this );
			if ( listOfServices == null ) {
				listOfServices = new ArrayList<>();
				listOfServices.add( service );
			}
		}

		public ObjectId getId() {
			return id;
		}

		public void setId(ObjectId id) {
			this.id = id;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public Integer getNumberOfServices() {
			return numberOfServices;
		}

		public void setNumberOfServices(Integer numberOfServices) {
			this.numberOfServices = numberOfServices;
		}

		public List<Service> getListOfServices() {
			return listOfServices;
		}

		public void setListOfServices(List<Service> listOfServices) {
			this.listOfServices = listOfServices;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ( ( id == null ) ? 0 : id.hashCode() );
			result = prime * result + ( ( listOfServices == null ) ? 0 : listOfServices.hashCode() );
			result = prime * result + ( ( name == null ) ? 0 : name.hashCode() );
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if ( this == obj ) {
				return true;
			}
			if ( obj == null ) {
				return false;
			}
			if ( getClass() != obj.getClass() ) {
				return false;
			}
			Lab other = (Lab) obj;
			if ( id == null ) {
				if ( other.id != null ) {
					return false;
				}
			}
			else if ( !id.equals( other.id ) ) {
				return false;
			}
			if ( listOfServices == null ) {
				if ( other.listOfServices != null ) {
					return false;
				}
			}
			else if ( !listOfServices.equals( other.listOfServices ) ) {
				return false;
			}
			if ( name == null ) {
				if ( other.name != null ) {
					return false;
				}
			}
			else if ( !name.equals( other.name ) ) {
				return false;
			}
			return true;
		}

		@Override
		public String toString() {
			return "Lab[" + id + ", " + name + ", " + listOfServices + "]";
		}
	}
}
