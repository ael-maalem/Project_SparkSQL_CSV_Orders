package me.elmaalem.project.model;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Optional;


public class Orders implements Serializable {

    private int orderId;
    private LocalDate date;
    private int quantity;
    private Optional<Double> sales;
    private String mode;
    private Optional<Double> profit;
    private double unitPrice;
    private String customerName;
    private String customerSegment;
    private String productCategory;
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("MM-dd-yyyy");

    public Orders(){}
    public Orders(int orderId, String date, int quantity, double sales, String mode, double profit, double unitPrice,
                  String customerName, String customerSegment, String productCategory) {
        this.orderId = orderId;
        this.date = LocalDate.parse(date,FORMATTER);
        this.quantity = quantity;
        this.sales = Optional.of(sales);
        this.mode = mode;
        this.profit = Optional.of(profit);
        this.unitPrice = unitPrice;
        this.customerName = customerName;
        this.customerSegment = customerSegment;
        this.productCategory = productCategory;
    }

    public String getProductCategory() {
        return productCategory;
    }

    public void setProductCategory(String productCategory) {
        this.productCategory = productCategory;
    }

    public int getOrderId() {
        return orderId;
    }

    public void setOrderId(int orderId) {
        this.orderId = orderId;
    }

    public LocalDate getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = LocalDate.parse(date,FORMATTER);
    }

    public int getQuantity() {
        return quantity;
    }

    public void setQuantity(int quantity) {
        this.quantity = quantity;
    }

    public Optional<Double> getSales() {
        return sales;
    }

    public void setSales(double sales) {
        this.sales = Optional.of(sales);
    }

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }

    public Optional<Double> getProfit() {
        return profit;
    }

    public void setProfit(double profit) {
        this.profit = Optional.of(profit);
    }

    public double getUnitPrice() {
        return unitPrice;
    }

    public void setUnitPrice(double unitPrice) {
        this.unitPrice = unitPrice;
    }

    public String getCustomerName() {
        return customerName;
    }

    public void setCustomerName(String customerName) {
        this.customerName = customerName;
    }

    public String getCustomerSegment() {
        return customerSegment;
    }

    public void setCustomerSegment(String customerSegment) {
        this.customerSegment = customerSegment;
    }


}
