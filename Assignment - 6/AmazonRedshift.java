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
 * java --add-opens java.base/java.net=ALL-UNNAMED -cp "Drivers/*;." AmazonRedshift
 */
public class AmazonRedshift {
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
        AmazonRedshift q = new AmazonRedshift();
        // Initialize the executor service for parallel execution
        executorService = Executors.newFixedThreadPool(4);
        ResultSet rs;
        while (true) {
            // Display menu
            System.out.println("\n===== Amazon Redshift Menu =====");
            System.out.println("1. Connect to the database");
            System.out.println("2. Drop all tables");
            System.out.println("3. Create schema and tables");
            System.out.println("4. Insert TPC-H Data");
            System.out.println("5. Execute Query 1");
            System.out.println("6. Execute Query 2");
            System.out.println("7. Execute Query 3");
            System.out.println("8. Close the connection");
            System.out.println("9. Exit");
            System.out.print("Enter your choice: ");
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
                        if (q.con == null) {
                            System.out.println("Please connect to the database first.");
                        } else {
                            try {
                                rs = q.query1();
                    
                                // Display header for the table
                                System.out.println("==========================================");
                                System.out.println("| Order Key   | Total Sale   | Order Date   |");
                                System.out.println("==========================================");
                    
                                // Process and display each row in the ResultSet
                                while (rs.next()) {
                                    System.out.printf("| %-12d | %-12.2f | %-13s |\n", 
                                                      rs.getInt("order_key"),        // Column alias from the query
                                                      rs.getDouble("total_sale"),   // Column alias from the query
                                                      rs.getDate("order_date"));    // Column alias from the query
                                }
                    
                                System.out.println("==========================================");
                            } catch (SQLException e) {
                                System.out.println("Error processing result set: " + e.getMessage());
                            }
                        }
                        break;
                    case 6:
                        try {
                            rs = q.query2();
                            if (rs != null) {
                                System.out.println("Query #2 Results:");
                                System.out.println("==========================================");
                                System.out.printf("%-15s %-20s%n", "Customer Key", "Total Spent");
                                System.out.println("==========================================");
                                while (rs.next()) {
                                    System.out.printf("%-15d %-20.2f%n", 
                                            rs.getInt("CustomerKey"), 
                                            rs.getDouble("TotalSpent"));
                                }
                                System.out.println("==========================================");
                            } else {
                                System.out.println("No results found for Query #2.");
                            }
                        } catch (SQLException e) {
                            System.out.println("Error executing Query #2: " + e.getMessage());
                        }
                        break;
                    case 7:
                    if (q.con == null) {
                        System.out.println("Please connect to the database first.");
                    } else {
                        rs = q.query3();
                        System.out.println("==========================================");
                        System.out.println("| Order Priority   | Line Item Count     |");
                        System.out.println("==========================================");
                        while (rs.next()) {
                            String orderPriority = rs.getString("O_ORDERPRIORITY");
                            int lineItemCount = rs.getInt("lineitem_count");
                            System.out.printf("| %-16s | %-18d |\n", orderPriority, lineItemCount);
                        }
                        System.out.println("==========================================");
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
            Class.forName("com.amazon.redshift.jdbc42.Driver");
            Properties properties = new Properties();
            properties.setProperty("user", masterUsername);
            properties.setProperty("password", password);

            this.con = DriverManager.getConnection(redshiftUrl, properties);

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
        String createSchemaQuery = "CREATE SCHEMA IF NOT EXISTS dev";
        try (Statement stmt = con.createStatement()) {
            stmt.executeUpdate(createSchemaQuery);
            System.out.println("Schema 'dev' created.");
        } catch (SQLException e) {
            System.out.println("Error creating schema: " + e.getMessage());
        }
        File ddlFolder = new File("ddl");
        File[] ddlFiles = ddlFolder.listFiles((dir, name) -> name.endsWith(".sql"));
        if (ddlFiles != null) {
            for (File ddlFile : ddlFiles) {
                try {
                    String ddlQuery = new String(Files.readAllBytes(ddlFile.toPath()));
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
                if (batchCount > 0) {
                    stmt.executeBatch();
                    recordCount += batchCount;
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
        String query = " SELECT o.O_ORDERKEY AS order_key, " +
                    " SUM(l.L_EXTENDEDPRICE) AS total_sale, " +
                    " o.O_ORDERDATE AS order_date " +
                    " FROM ORDERS o " +
                    " JOIN LINEITEM l ON o.O_ORDERKEY = l.L_ORDERKEY " +
                    " JOIN CUSTOMER c ON o.O_CUSTKEY = c.C_CUSTKEY " +
                    " JOIN NATION n ON c.C_NATIONKEY = n.N_NATIONKEY " +
                    " WHERE n.N_NAME = 'UNITED STATES' " +
                    " GROUP BY o.O_ORDERKEY, o.O_ORDERDATE " +
                    " ORDER BY o.O_ORDERDATE DESC " +
                    " LIMIT 10 ";
        if (con == null) {
            throw new SQLException("Connection is null. Please connect to the database first.");
        }
    
        Statement stmt = con.createStatement();
        return stmt.executeQuery(query);
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
        String segmentQuery = "SELECT C_MKTSEGMENT " +
                              "FROM CUSTOMER " +
                              "GROUP BY C_MKTSEGMENT " +
                              "ORDER BY COUNT(C_CUSTKEY) DESC " +
                              "LIMIT 1";
    
        Statement stmt = con.createStatement();
        ResultSet segmentResult = stmt.executeQuery(segmentQuery);
        String largestSegment = null;
        if (segmentResult.next()) {
            largestSegment = segmentResult.getString("C_MKTSEGMENT");
        }
        if (largestSegment != null) {
            String query = "WITH NonEuropeanCustomers AS ( " +
                           "    SELECT C.C_CUSTKEY " +
                           "    FROM CUSTOMER C " +
                           "    JOIN NATION N ON C.C_NATIONKEY = N.N_NATIONKEY " +
                           "    JOIN REGION R ON N.N_REGIONKEY = R.R_REGIONKEY " +
                           "    WHERE R.R_NAME != 'EUROPE' " +
                           "), " +
                           "FilteredCustomers AS ( " +
                           "    SELECT C.C_CUSTKEY " +
                           "    FROM CUSTOMER C " +
                           "    WHERE C.C_MKTSEGMENT = ? " +
                           "    AND C.C_CUSTKEY IN (SELECT C_CUSTKEY FROM NonEuropeanCustomers) " +
                           "), " +
                           "UrgentOrders AS ( " +
                           "    SELECT O.O_CUSTKEY AS CustomerKey, SUM(L.L_EXTENDEDPRICE) AS TotalSpent " +
                           "    FROM ORDERS O " +
                           "    JOIN LINEITEM L ON O.O_ORDERKEY = L.L_ORDERKEY " +
                           "    WHERE O.O_ORDERPRIORITY = '1-URGENT' " +
                           "      AND O.O_ORDERSTATUS != 'F' " +
                           "      AND O.O_CUSTKEY IN (SELECT C_CUSTKEY FROM FilteredCustomers) " +
                           "    GROUP BY O.O_CUSTKEY " +
                           ") " +
                           "SELECT U.CustomerKey, U.TotalSpent " +
                           "FROM UrgentOrders U " +
                           "ORDER BY U.TotalSpent DESC";
    
            PreparedStatement preparedStatement = con.prepareStatement(query);
            preparedStatement.setString(1, largestSegment); // Set the largest market segment dynamically
            return preparedStatement.executeQuery();
        }
        System.out.println("No largest market segment found.");
        return null;
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
        String query = "SELECT o.O_ORDERPRIORITY, COUNT(l.L_LINENUMBER) AS lineitem_count " +
                       "FROM orders o " +
                       "JOIN lineitem l ON o.O_ORDERKEY = l.L_ORDERKEY " +
                       "WHERE o.O_ORDERDATE >= '1997-04-01' " +
                       "AND o.O_ORDERDATE < DATEADD(year, 6, '1997-04-01') " +
                       "GROUP BY o.O_ORDERPRIORITY " +
                       "ORDER BY o.O_ORDERPRIORITY ASC";
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