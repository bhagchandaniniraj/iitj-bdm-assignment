import java.math.BigDecimal;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Performs SQL DDL and SELECT queries on a MySQL database hosted on AWS RDS.
 * 
 * java --add-opens java.base/java.net=ALL-UNNAMED -cp "Drivers/*;." ARedShift
 */
class ARedShift {
    /**
     * Connection to database
     */
    static final String redshiftUrl = "jdbc:redshift://g23ai2087-cluster.cndskybccs1r.us-east-1.redshift.amazonaws.com:5439/dev";
    static final String masterUsername = "admin"; // Replace with your Redshift admin username
    static final String password = "IITj1234"; // Replace with your Redshift password
    private static ExecutorService executorService;
    /**
     * Main method is only used for convenience. Use JUnit test file to verify your
     * answer.
     *
     * @param args
     *             none expected
     * @throws SQLException
     *                      if a database error occurs
     */
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        ARedShift q = new ARedShift();
        // Initialize the executor service for parallel execution
        executorService = Executors.newFixedThreadPool(4); // Adjust the number of threads based on your system
        ResultSet rs;
        while (true) {
            // Display menu
            System.out.println("\n===== Amazon Redshift Menu =====");
            System.out.println("\n===== Amazon Redshift Management Menu =====");
            System.out.println("1. Connect to the database (Establish a connection to Redshift)");
            System.out.println("2. Drop all tables (Remove existing tables from the schema)");
            System.out.println("3. Create schema and tables (Set up the database structure)");
            System.out.println("4. Insert TPC-H Data (Load sample data into the database)");
            System.out.println("5. Execute Query 1 (Retrieve recent top 10 orders by total sale)");
            System.out.println("6. Execute Query 2 (Analyze customer spending outside Europe)");
            System.out.println("7. Execute Query 3 (Count line items grouped by order priority)");
            System.out.println("8. Close the connection (Terminate database connection)");
            System.out.println("9. Exit (Exit the program)");
            System.out.print("Enter your choice: ");
            // Read user input
            int choice = scanner.nextInt();

            try {
                switch (choice) {
                    case 1:
                        q.connect();
                        break;
                    case 2:
                        q.drop();
                        break;
                    case 3:
                        q.create();
                        break;
                    case 4:
                        q.insert();
                        break;
                    case 5:
                        rs = q.query1();
                        try {
                            while (rs.next()) {
                                System.out.println("Order Key: " + rs.getInt("orderkey") + 
                                                   ", Total Sale: " + rs.getDouble("total_sale") +
                                                   ", Order Date: " + rs.getDate("orderdate"));
                            }
                        } catch (SQLException e) {
                            System.out.println("Error processing result set: " + e.getMessage());
                        }
                        break;
                    case 6:
                            // Check if ResultSet is not null and has results
                        rs = q.query2();
                        try{
                            if (rs != null) {
                                // Display the results from the ResultSet
                                System.out.println("Customer Key | Total Amount Spent");
                                while (rs.next()) {
                                    int customerKey = rs.getInt("C_CUSTKEY");
                                    double totalSpent = rs.getDouble("total_spent");
                                    System.out.println(customerKey + " | " + totalSpent);
                                }
                            } else {
                                System.out.println("No results found for the query.");
                            }
        
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                        break;
                    case 7:
                        if (q.con == null) {
                            System.out.println("Please connect to the database first.");
                        } else {
                            rs = q.query3();
                            System.out.println("Order Priority | Line Item Count");
                            System.out.println("-------------------------------");
                            while (rs.next()) {
                                String orderPriority = rs.getString("O_ORDERPRIORITY");
                                int lineItemCount = rs.getInt("lineitem_count");
                                System.out.println(orderPriority + " | " + lineItemCount);
                            }
                        }
                        break;
                    case 8:
                        q.close();
                        break;
                    case 9:
                        System.out.println("Exiting program...");
                        scanner.close();
                        System.exit(0);  // Exit the program
                        break;
                    default:
                        System.out.println("Invalid choice. Please try again.");
                        break;
                }
            } catch (SQLException e) {
                System.out.println("Error: " + e.getMessage());
            } catch (Exception e) {
                System.out.println("An unexpected error occurred: " + e.getMessage());
            }
        }
    }

    /**
     * Makes a connection to the database and returns connection to caller.
     *
     * @return
     *         connection
     * @throws SQLException
     *                      if an error occurs
     */
    
    // Redshift connection details
    
    
    private Connection con;

    public Connection connect() {
        try {
            // Register Redshift JDBC driver
            Class.forName("com.amazon.redshift.jdbc42.Driver");

            // Create connection properties
            Properties properties = new Properties();
            properties.setProperty("user", masterUsername);
            properties.setProperty("password", password);

            // Establish connection
            con = DriverManager.getConnection(redshiftUrl, properties);

            System.out.println("Connection to Redshift established successfully.");
        } catch (ClassNotFoundException e) {
            System.out.println("Error: Redshift JDBC driver not found.");
            e.printStackTrace();
        } catch (SQLException e) {
            System.out.println("Failed to connect to the database.");
            e.printStackTrace();
        }
        return con;
    }
    /**
     * Closes connection to database.
     */
    public void close() {
        System.out.println("Closing database connection.");
        try {
            if (con != null && !con.isClosed()) {
                con.close();
            }
        } catch (SQLException e) {
            System.out.println("Error closing the connection.");
        }
    }

    public void drop() {
        System.out.println("Dropping all tables in the 'dev' schema...");
        String dropQuery = "SELECT tablename FROM pg_tables WHERE schemaname = 'dev'";

        try (Statement stmt = con.createStatement()) {
            ResultSet rs = stmt.executeQuery(dropQuery);
            while (rs.next()) {
                String tableName = rs.getString("tablename");
                String dropTableQuery = "DROP TABLE IF EXISTS dev." + tableName;
                stmt.executeUpdate(dropTableQuery);
                System.out.println("Dropped table: " + tableName);
            }
        } catch (SQLException e) {
            System.out.println("Error dropping tables: " + e.getMessage());
        }
    }

    public void create() throws SQLException {
        System.out.println("Creating the 'dev' schema and tables...");
    
        // Create schema if not exists
        String createSchemaQuery = "CREATE SCHEMA IF NOT EXISTS dev";
        try (Statement stmt = con.createStatement()) {
            stmt.executeUpdate(createSchemaQuery);
            System.out.println("Schema 'dev' created.");
        } catch (SQLException e) {
            System.out.println("Error creating schema: " + e.getMessage());
        }
    
        // Create tables using DDL files located in 'ddl' folder
        File ddlFolder = new File("ddl");
        File[] ddlFiles = ddlFolder.listFiles((dir, name) -> name.endsWith(".sql"));
        if (ddlFiles != null) {
            for (File ddlFile : ddlFiles) {
                try {
                    // Read the contents of the DDL file
                    String ddlQuery = new String(Files.readAllBytes(ddlFile.toPath()));
    
                    // Special handling for 'tpch_create.sql'
                    if (ddlFile.getName().equals("tpch_create.sql")) {
                        // Ensure the 'tpch_create.sql' contains CREATE TABLE queries
                        if (ddlQuery.toUpperCase().contains("CREATE TABLE")) {
                            try (Statement stmt = con.createStatement()) {
                                stmt.executeUpdate(ddlQuery);
                                System.out.println("Created table(s) from " + ddlFile.getName());
                            }
                        } else {
                            System.out.println("No CREATE TABLE queries found in tpch_create.sql.");
                        }
                    } else {
                        // For other files, you can add different logic based on the content
                        System.out.println("Skipping non-CREATE TABLE SQL file: " + ddlFile.getName());
                    }
                } catch (IOException e) {
                    System.out.println("Error reading DDL file: " + ddlFile.getName() + " - " + e.getMessage());
                } catch (SQLException e) {
                    System.out.println("Error executing DDL query for file " + ddlFile.getName() + " - " + e.getMessage());
                }
            }
        } else {
            System.out.println("No DDL files found in 'ddl' folder.");
        }
    }

    // Insert TPC-H Data (modified to show progress and wait until all insertions are done)
    public void insert() {
        File dataFolder = new File("ddl");
        File[] dataFiles = dataFolder.listFiles((dir, name) -> name.endsWith(".sql") && !name.equals("tpch_create.sql"));

        if (dataFiles != null) {
            // Use a CountDownLatch to wait for all insertions to complete
            CountDownLatch latch = new CountDownLatch(dataFiles.length);

            for (File dataFile : dataFiles) {
                executorService.submit(() -> {
                    processFile(dataFile);
                    latch.countDown();  // Decrease latch count when each file is processed
                });
            }

            try {
                // Wait until all insert tasks are completed
                latch.await();
                System.out.println("All data insertions completed.");
            } catch (InterruptedException e) {
                System.out.println("Error waiting for insertions to complete: " + e.getMessage());
            }
        } else {
            System.out.println("No data files found in the folder.");
        }
    }
    private void processFile(File dataFile) {
        try {
            // Read the SQL query from the file
            String sqlQuery = new String(Files.readAllBytes(dataFile.toPath()));
    
            // Log the start of the process
            System.out.println("Processing file: " + dataFile.getName());
    
            // Open the Statement for executing the insert query
            try (Statement stmt = con.createStatement()) {
                // Split the SQL query into individual statements (assuming multiple INSERT statements in the file)
                String[] insertStatements = sqlQuery.split(";");
                int totalStatements = insertStatements.length; // Total number of statements in the file
    
                int batchCount = 0;
                int recordCount = 0;
    
                // Process each INSERT statement
                for (String statement : insertStatements) {
                    if (statement.trim().isEmpty()) continue;
    
                    // Add the statement to the batch
                    stmt.addBatch(statement.trim());
                    batchCount++;
    
                    // Execute the batch every 500 records
                    if (batchCount % 500 == 0) {
                        stmt.executeBatch(); // Execute the batch
                        recordCount += 500;
    
                        // Calculate and display progress
                        double percentageCompleted = (recordCount / (double) totalStatements) * 100;
                        int remainingRecords = totalStatements - recordCount;
                        System.out.printf("File: %s | Inserted: %d | Remaining: %d | Progress: %.2f%%%n",
                                dataFile.getName(), recordCount, remainingRecords, percentageCompleted);
                    }
                }
    
                // Execute any remaining statements after finishing the loop
                if (batchCount > 0) {
                    stmt.executeBatch();
                    recordCount += batchCount;
    
                    // Calculate and display final progress
                    double percentageCompleted = (recordCount / (double) totalStatements) * 100;
                    int remainingRecords = totalStatements - recordCount;
                    System.out.printf("File: %s | Inserted: %d | Remaining: %d | Progress: %.2f%%%n",
                            dataFile.getName(), recordCount, remainingRecords, percentageCompleted);
                }
    
                System.out.println("Insertion completed for file: " + dataFile.getName());
    
            } catch (SQLException e) {
                System.out.println("Error executing statement: " + e.getMessage());
                e.printStackTrace();
            }
    
        } catch (IOException e) {
            System.out.println("Error reading file: " + dataFile.getName() + " - " + e.getMessage());
            e.printStackTrace(); // Optional: log the stack trace for more details
        }
    }
    

    // Process each data file (assuming SQL insert file with multiple records to insert)
    //For Jatin
    // private void processFile(File dataFile) {
    //     try {
    //         // Read the SQL query from the file
    //         String sqlQuery = new String(Files.readAllBytes(dataFile.toPath()));
    
    //         // Log the start of the process
    //         System.out.println("Processing file: " + dataFile.getName());
    
    //         // Open the Statement for executing the insert query
    //         try (Statement stmt = con.createStatement()) {
    //             // Split the SQL query into individual statements (assuming multiple INSERT statements in the file)
    //             String[] insertStatements = sqlQuery.split(";");
    
    //             int batchCount = 0;
    //             int recordCount = 0;
    
    //             // Process each INSERT statement
    //             for (String statement : insertStatements) {
    //                 if (statement.trim().isEmpty()) continue;
    
    //                 // Execute the statement directly (for batch processing, use addBatch() on Statement)
    //                 stmt.addBatch(statement.trim());  // Add to batch
    
    //                 batchCount++;
    
    //                 // Execute the batch every 100 records
    //                 if (batchCount % 500 == 0) {
    //                     stmt.executeBatch();  // Execute the batch of 100 records
    //                     recordCount += 500;
    //                     System.out.println("Processed " + recordCount + " records for file: " + dataFile.getName());
    //                 }
    //             }
    
    //             // Execute any remaining batch after finishing the loop
    //             if (batchCount > 0) {
    //                 stmt.executeBatch();  // Execute remaining batch
    //                 recordCount += batchCount;
    //                 System.out.println("Processed " + recordCount + " records for file: " + dataFile.getName());
    //             }
    
    //             System.out.println("Insertion completed for file: " + dataFile.getName());
    
    //         } catch (SQLException e) {
    //             System.out.println("Error executing statement: " + e.getMessage());
    //             e.printStackTrace();
    //         }
    
    //     } catch (IOException e) {
    //         System.out.println("Error reading file: " + dataFile.getName() + " - " + e.getMessage());
    //         e.printStackTrace();  // Optional: log the stack trace for more details
    //     }
    // }
    

    /**
     * Query returns the most recent top 10 orders with the total sale and the date
     * of the
     * order in `America`.
     *
     * @return
     *         ResultSet
     * @throws SQLException
     *                      if an error occurs
     */
    public ResultSet query1() throws SQLException {
        System.out.println("Executing query #1.");
    
        // SQL query to get the top 10 most recent orders from customers in America
        String query = "SELECT o.o_orderkey, SUM(l.l_extendedprice) AS total_sale, o.o_orderdate " +
               "FROM orders o " +
               "JOIN lineitem l ON o.o_orderkey = l.l_orderkey " +
               "JOIN customer c ON o.o_custkey = c.c_custkey " +
               "JOIN nation n ON c.c_nationkey = n.n_nationkey " +
               "WHERE n.n_name = 'America' " +
               "GROUP BY o.o_orderkey, o.o_orderdate " +
               "ORDER BY o.o_orderdate DESC " +
               "LIMIT 10";
    
        // Check if connection is null
        if (con == null) {
            throw new SQLException("Connection is null. Please connect to the database first.");
        }
    
        // Execute the query
        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery(query);
    
        return rs;
    }
    

    /**
     * Query returns the customer key and the total price a customer spent in
     * descending
     * order, for all urgent orders that are not failed for all customers who are
     * outside Europe belonging
     * to the highest market segment.
     *
     * @return
     *         ResultSet
     * @throws SQLException
     *                      if an error occurs
     */
    public ResultSet query2() throws SQLException {
        System.out.println("Executing query #2.");
    
        // Step 1: Find the largest market segment (the segment with the most customers)
        String segmentQuery = "SELECT C_MKTSEGMENT " +
                              "FROM customer " +
                              "GROUP BY C_MKTSEGMENT " +
                              "ORDER BY COUNT(*) DESC " +
                              "LIMIT 1";
    
        Statement stmt = con.createStatement();
        ResultSet segmentResult = stmt.executeQuery(segmentQuery);
        String largestSegment = null;
    
        if (segmentResult.next()) {
            largestSegment = segmentResult.getString("C_MKTSEGMENT");
        }
    
        // Step 2: Query to get customer key and total amount spent for urgent orders
        // (not failed), located outside Europe, and belonging to the largest market segment
        if (largestSegment != null) {
            String query = "SELECT c.C_CUSTKEY, SUM(l.L_EXTENDEDPRICE) AS total_spent " +
                           "FROM orders o " +
                           "JOIN lineitem l ON o.O_ORDERKEY = l.L_ORDERKEY " +
                           "JOIN customer c ON o.O_CUSTKEY = c.C_CUSTKEY " +
                           "JOIN nation n ON c.C_NATIONKEY = n.N_NATIONKEY " +  // Join with nation to filter European countries
                           "WHERE c.C_MKTSEGMENT = ? " + // Filter by the largest market segment
                           "AND n.N_NAME NOT IN ('Germany', 'France', 'Italy', 'Spain', 'UK') " + // Exclude European countries
                           "AND o.O_ORDERSTATUS = 'A' " + // Only urgent orders (status 'A' assumed to be urgent)
                           "AND o.O_ORDERSTATUS != 'F' " + // Exclude failed orders (status 'F' assumed to be failed)
                           "GROUP BY c.C_CUSTKEY " +
                           "ORDER BY total_spent DESC";
    
            PreparedStatement preparedStatement = con.prepareStatement(query);
            preparedStatement.setString(1, largestSegment); // Set the market segment as a parameter
    
            ResultSet rs = preparedStatement.executeQuery();
            return rs;
        }
    
        return null; // If no largest segment found
    }
    /**
     * Query returns all the lineitems that was ordered within the six years from
     * January 4th,
     * 1997 and the orderpriority in ascending order.
     *
     * @return
     *         ResultSet
     * @throws SQLException
     *                      if an error occurs
     */
    public ResultSet query3() throws SQLException {
        System.out.println("Executing query #3.");
    
        // SQL query to count line items grouped by order priority
        String query = "SELECT o.O_ORDERPRIORITY, COUNT(l.L_LINENUMBER) AS lineitem_count " +
                       "FROM orders o " +
                       "JOIN lineitem l ON o.O_ORDERKEY = l.L_ORDERKEY " +
                       "WHERE o.O_ORDERDATE >= '1997-04-01' " +
                       "AND o.O_ORDERDATE < DATEADD(year, 6, '1997-04-01') " +
                       "GROUP BY o.O_ORDERPRIORITY " +
                       "ORDER BY o.O_ORDERPRIORITY ASC";
    
        // Execute the query
        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery(query);
    
        return rs;
    }
    

    /*
     * Do not change anything below here.
     */
    /**
     * Converts a ResultSet to a string with a given number of rows displayed.
     * Total rows are determined but only the first few are put into a string.
     *
     * @param rst
     *                ResultSet
     * @param maxrows
     *                maximum number of rows to display
     * @return
     *         String form of results
     * @throws SQLException
     *                      if a database error occurs
     */
    public static String resultSetToString(ResultSet rst, int maxrows) throws SQLException {
        StringBuffer buf = new StringBuffer(5000);
        int rowCount = 0;
        ResultSetMetaData meta = rst.getMetaData();
        buf.append("Total columns: " + meta.getColumnCount());
        buf.append('\n');
        if (meta.getColumnCount() > 0)
            buf.append(meta.getColumnName(1));
        for (int j = 2; j <= meta.getColumnCount(); j++)
            buf.append(", " + meta.getColumnName(j));
        buf.append('\n');
        while (rst.next()) {
            if (rowCount < maxrows) {
                for (int j = 0; j < meta.getColumnCount(); j++) {
                    Object obj = rst.getObject(j + 1);
                    buf.append(obj);
                    if (j != meta.getColumnCount() - 1)
                        buf.append(", ");
                }
                buf.append('\n');
            }
            rowCount++;
        }
        buf.append("Total results: " + rowCount);
        return buf.toString();
    }

    /**
     * Converts ResultSetMetaData into a string.
     *
     * @param meta
     *             ResultSetMetaData
     * @return
     *         string form of metadata
     * @throws SQLException
     *                      if a database error occurs
     */
    public static String resultSetMetaDataToString(ResultSetMetaData meta) throws SQLException {
        StringBuffer buf = new StringBuffer(5000);
        buf.append(meta.getColumnName(1) + " (" + meta.getColumnLabel(1) + ", " +
                meta.getColumnType(1) + "-" + meta.getColumnTypeName(1) + ", " +
                meta.getColumnDisplaySize(1) + ", " + meta.getPrecision(1) + ", " +
                meta.getScale(1) + ")");
        for (int j = 2; j <= meta.getColumnCount(); j++) {
            buf.append(", " + meta.getColumnName(j) + " (" + meta.getColumnLabel(j) + ", " +
                    meta.getColumnType(j) + "-" + meta.getColumnTypeName(j) + ", " +
                    meta.getColumnDisplaySize(j) + ", " + meta.getPrecision(j) + ", " +
                    meta.getScale(j) + ")");
        }
        return buf.toString();
    }
}