/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hibernate.datastore.ogm.orientdb.jpa;

import com.orientechnologies.orient.core.id.ORecordId;
import java.util.List;
import java.util.Objects;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.OneToMany;
import javax.persistence.Version;
import org.hibernate.search.annotations.Analyze;
import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.FieldBridge;
import org.hibernate.search.annotations.Index;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.annotations.Store;
import org.hibernate.search.orientdb.bridge.ORecordIdTwoWayStringBridge;

/**
 *
 * @author chernolyassv
 */
@Entity
@Indexed(index = "Customer")
@NamedQueries({
    @NamedQuery(name = "Customer.findAll",
            query = "SELECT c FROM Customer c"),
    @NamedQuery(name = "Country.findByName",
            query = "SELECT c FROM Customer c WHERE c.name = :name")})
public class Customer {

    @Id
    @Column(name = "@rid")
    //@GeneratedValue(strategy=GenerationType.AUTO)
    @FieldBridge(impl = ORecordIdTwoWayStringBridge.class)
    private ORecordId id  =ORecordId.EMPTY_RECORD_ID;
    @Field(index = Index.YES, analyze = Analyze.YES, store = Store.YES)
    private String name;
    @OneToMany(mappedBy = "owner")
    private List<BuyingOrder> orders;
    
    @Version
    @Column(name = "@version")
    private int version;
    
    public ORecordId getId() {
        return id;
    }

    public void setId(ORecordId id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<BuyingOrder> getOrders() {
        return orders;
    }

    public void setOrders(List<BuyingOrder> orders) {
        this.orders = orders;
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 47 * hash + Objects.hashCode(this.id);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Customer other = (Customer) obj;
        if (!Objects.equals(this.id, other.id)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "Customer{" + "id=" + id + ", name=" + name + '}';
    }
}
