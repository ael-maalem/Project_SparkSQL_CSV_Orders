package me.elmaalem.project.main;

import me.elmaalem.project.service.SparkService;
import java.util.Scanner;

public class Menu {

    public static Scanner scanner = new Scanner(System.in);
    public static SparkService sparkService = new SparkService();

    public static void main(String[] args) {

        try {
            int menuOption = 0;
            String customerName;
            String productCategory;
            String orderDate;
            String startDate;
            String endDate;

            do {
                // Setting menuOption equal to return value from showMenu();
                menuOption = showMenu();

                switch (menuOption) {

                    case 1:
                        System.out.println("Enter the rows number of Orders that you want show : ");
                        int numberRows = scanner.nextInt();
                        sparkService.listOrders(numberRows);
                        break;
                    case 2:
                        System.out.println("Enter the customer name :  Ex= Liz Pelletier");
                        scanner.nextLine();//// Adding nextLine just to discard the old \n character
                        customerName = scanner.nextLine();
                        sparkService.listOrdersMatchCustomerName (customerName);
                        break;
                    case 3:
                        System.out.println("Enter the customer name : Ex= Liz Pelletier");
                        scanner.nextLine();
                        customerName = scanner.nextLine();
                        System.out.println("Enter the order date with this format (yyyy-MM-dd) : Ex= 2008-07-15");
                        orderDate = scanner.nextLine();
                        sparkService.listOrdersMatchCustomerNameAndOrderDate (customerName,orderDate);
                        break;
                    case 4:
                        System.out.println("Enter the product category : Ex: Office Supplies");
                        scanner.nextLine();
                        productCategory = scanner.nextLine();
                        sparkService.listOrdersMatchCategoryAndProfitPositiveAndSortByCustomerName(productCategory);
                        break;
                    case 5:
                        System.out.println("Enter the product category : Ex: Office Supplies");
                        scanner.nextLine();
                        productCategory = scanner.nextLine();
                        System.out.println("Enter the start date : Ex: 2010-03-16");
                        startDate = scanner.nextLine();
                        System.out.println("Enter the end date : Ex: 2011-08-31");
                        endDate = scanner.nextLine();
                        sparkService.listOrdersMatchCategoryAndPeriodDateAndSortBySales (productCategory,startDate,endDate);
                        break;
                    case 6:
                        System.out.println("Quitting Program...");
                        break;
                    default:
                        System.out.println("Sorry, please enter valid Option");
                }

            } while (menuOption != 6);

            // Exiting message when user decides to quit Program
            System.out.println("Thanks for using this Program...");

        } catch (Exception ex) {
            System.out.println("Sorry problem occured within Program");
            scanner.next();
        } finally {
            scanner.close();
        }
    }
    public static int showMenu() {
        int option = 0;

        System.out.println("Menu:");
        System.out.println("1. Get All Orders form CSV file ");
        System.out.println("2. Get Orders By Customer Name");
        System.out.println("3. Get Orders(Name,Sales,Profit,Category)  By Customer Name and Order Date");
        System.out.println("4. Get Orders By Product Category When Profit must be greater than 0 And Sorted by Customer Name");
        System.out.println("5. Get Orders By Product Category In period and sorted by Sales");
        System.out.println("6. Quit Program");

        // Getting query number from above menu
        System.out.println("Enter the number of Query from above...");
        option = scanner.nextInt();

        return option;
    }
}
