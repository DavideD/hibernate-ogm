/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hibernate.datastore.ogm.orientdb.util;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import com.tinkerpop.blueprints.impls.orient.OrientVertexType;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author chernolyassv
 */
public class MemoryDBUtil {

    public static Map<String,ORecordId> prepareDb(String url) {
        Map<String,ORecordId> idMap = new HashMap<>();
        OrientGraphFactory factory = new OrientGraphFactory(url);

        OrientGraphNoTx graph = factory.getNoTx();

        // vertex classes        
        OrientVertexType pizzaType = graph.createVertexType("Pizza");
        pizzaType.createProperty("name", OType.STRING);
        for (OProperty p : pizzaType.declaredProperties()) {
            System.out.println("Property: " + p);
        }

        OrientVertexType orderType = graph.createVertexType("Order");
        orderType.createProperty("orderKey", OType.STRING);
        OrientVertexType orderItemType = graph.createVertexType("OrderItem");
        orderItemType.createProperty("cost", OType.DECIMAL);

        OrientVertexType customerType = graph.createVertexType("Customer");
        customerType.createProperty("name", OType.STRING);

        // create vertex
        Vertex pizza = graph.addVertex("class:Pizza");
        pizza.setProperty("name", "Super Papa");
        System.out.println("pizza.getId():" + pizza.getId());
        idMap.put("Pizza", (ORecordId) pizza.getId());

        Vertex customer = graph.addVertex("class:Customer");
        customer.setProperty("name", "Ivahoe");
        System.out.println("customer.getId():" + customer.getId());
        idMap.put("Customer", (ORecordId) customer.getId());

        Vertex order = graph.addVertex("class:Order");
        order.setProperty("orderKey", "2233");
        System.out.println("order.getId():" + order.getId());
        idMap.put("Order", (ORecordId) order.getId());

        Vertex orderItem = graph.addVertex("class:OrderItem");
        orderItem.setProperty("cost", new BigDecimal("10.2"));
        System.out.println("orderItem.getId():" + orderItem.getId());
        idMap.put("OrderItem", (ORecordId) orderItem.getId());

        // create edges        
        graph.addEdge(null, pizza, orderItem, "commodites");
        graph.addEdge(null, orderItem, order, "orderItems");
        graph.addEdge(null, customer, order, "customerOrders");
        return idMap;
    }

}
