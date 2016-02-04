/*
 * Hibernate OGM, Domain model persistence for NoSQL datastores
 * 
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */

package org.hibernate.datastore.ogm.orientdb.jpa;

import com.orientechnologies.orient.core.id.ORecordId;
import java.math.BigDecimal;
import java.util.Objects;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.ManyToOne;
import javax.persistence.Version;
import org.hibernate.search.annotations.FieldBridge;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.orientdb.bridge.ORecordIdTwoWayStringBridge;

/**
 *
 * @author Sergey Chernolyas <sergey.chernolyas@gmail.com>
 */

@Entity
@Indexed(index = "OrderItem")
public class OrderItem {
    @Id
    @Column(name = "@rid")
    @GeneratedValue(strategy=GenerationType.IDENTITY)
    @FieldBridge(impl = ORecordIdTwoWayStringBridge.class)
    private ORecordId id;
    
    private BigDecimal cost;
    @ManyToOne
    private BuyingOrder order;
    @ManyToOne
    private Pizza buying;
    @Version
    @Column(name = "@version")
    private int version;

    public ORecordId getId() {
        return id;
    }

    public void setId(ORecordId id) {
        this.id = id;
    }

    public BigDecimal getCost() {
        return cost;
    }

    public void setCost(BigDecimal cost) {
        this.cost = cost;
    }

    public BuyingOrder getOrder() {
        return order;
    }

    public void setOrder(BuyingOrder order) {
        this.order = order;
    }

    public Pizza getBuying() {
        return buying;
    }

    public void setBuying(Pizza buying) {
        this.buying = buying;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 97 * hash + Objects.hashCode(this.id);
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
        final OrderItem other = (OrderItem) obj;
        if (!Objects.equals(this.id, other.id)) {
            return false;
        }
        return true;
    }
    
    
    
}
