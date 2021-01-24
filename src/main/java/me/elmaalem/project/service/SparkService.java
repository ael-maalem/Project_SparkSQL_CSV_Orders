package me.elmaalem.project.service;

import me.elmaalem.project.model.Orders;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static me.elmaalem.project.repository.EntryPoint.getDataset;

public class SparkService {

    public void listOrders(int numberRows){
        getDataset().show(numberRows);
    }

    public void listOrdersMatchCustomerName(String customerName) {
        Dataset<Orders> orders = getDataset().filter((FilterFunction<Orders>) order -> order.getCustomerName().equals(customerName));
        orders.show((int) getDataset().count());
    }

    public void listOrdersMatchCustomerNameAndOrderDate(String customerName, String orderDate) {
        Dataset<Row> orders = getDataset()
                .filter((FilterFunction<Orders>) order -> order.getCustomerName().equals(customerName))
                .select("customerName","date","sales","profit","productCategory")
                .where("date == \"" + orderDate + "\"");
        orders.show((int) getDataset().count());
    }

    public void listOrdersMatchCategoryAndProfitPositiveAndSortByCustomerName(String productCategory) {
        Dataset<Orders> orders = getDataset()
                .filter((FilterFunction<Orders>) order -> order.getProductCategory().equals(productCategory))
                .filter("profit > 0.0")
                .sort("customerName");
        orders.show((int) getDataset().count());
    }

    public void listOrdersMatchCategoryAndPeriodDateAndSortBySales(String productCategory, String startDate, String endDate) {
        Dataset<Row> orders = getDataset()
                .filter((FilterFunction<Orders>) order -> order.getProductCategory().equals("Office Supplies"))
                .sort("sales")
                .where("date < \"2011-08-31\" and date > \"2010-03-16\"")
                .select("customerName","date","sales","quantity","profit","unitPrice","customerSegment");
        orders.show((int) getDataset().count());
    }
}
