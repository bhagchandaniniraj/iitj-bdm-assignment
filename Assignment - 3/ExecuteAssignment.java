
import java.util.Scanner;
import java.lang.*;

/*
D:\IIT - Jodhpur\BigData>javac -cp mysql.jar;. *.java
D:\IIT - Jodhpur\BigData>java -cp mysql.jar;. ExecuteAssignment      
 */
class ExecuteAssignment{
    private BigDataAssign bda;
    public BigDataAssign getBda(){
        return this.bda;
    }

    public ExecuteAssignment(){
        System.out.println("Hello World");
        this.bda = new BigDataAssign();
        bda.connect();
        bda.createDatabase();
    }

    public static void main(String args[]){
        ExecuteAssignment e = new ExecuteAssignment();

        Scanner scanner = new Scanner(System.in);
        boolean exit = false;
        while (!exit) {
            System.out.println("\nMenu:");
            System.out.println("1. Connect to Database");
            System.out.println("2. Create Database");
            System.out.println("3. Create Table");
            System.out.println("4. Insert into tables");
            System.out.println("5. queryOne");
            System.out.println("6. queryTwo");
            System.out.println("7. queryThree");
            System.out.println("8. Drop Tables");
            System.out.println("9. Delete Database");
            System.out.println("10. Result Set ToString");
            System.out.println("11. Result Set Meta Data To String");
            System.out.println("12. Exit");
            System.out.print("Enter your choice: ");
            
            int choice = scanner.nextInt();
            switch (choice) {
                case 1 :
                    System.out.println("Connecting the Database...\n");
                    e.bda.connect();
                    break;
                case 2 :
                    e.getBda().createDatabase();
                    System.out.println("Database Created Successfully ...\n");
                    break;
                case 3 :
                    System.out.println("Creating the Tables...\n");
                    e.getBda().createTable();
                    break;
                case 4 :
                    System.out.println("Inserting Data...");
                    e.getBda().insertData();
                    break;
                case 5 :
                    System.out.println("Executing Simple One Query");
                    e.getBda().queryOne();
                    break;
                case 6 :
                    System.out.println("Executing Simple Two Query");
                    e.getBda().queryTwo();
                    break;
                case 7 :
                    System.out.println("Executing Simple Three Query");
                    e.getBda().queryThree();
                    break;
                case 8 :
                    System.out.println("Drop Tables");
                    e.getBda().dropTables();
                    break;
                case 9 :
                    System.out.println("Dropping Database");
                    e.getBda().dropDB();
                    break;
                case 10 :
                    System.out.println("Result Set to Data String");
                    e.getBda().resultSetToString("company");
                    e.getBda().resultSetToString("stockprice");
                    break;
                case 11 :
                    e.getBda().resultSetMetaDataToString("company");
                    e.getBda().resultSetMetaDataToString("stockprice");
                    break;
                case 12 :
                    exit = true;
                    break;
                default : 
                    System.out.println("Invalid choice. Try again.");
                    break;
            }
            //scanner.close();
        }
    }
}