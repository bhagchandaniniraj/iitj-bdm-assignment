import com.google.api.gax.rpc.NotFoundException;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.Mutation;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.admin.v2.models.ModifyColumnFamiliesRequest;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.RowMutation;

import java.util.ArrayList;
import java.util.Scanner;
import java.io.*;


public class Bigtable {
    // Configuration details
    public final String projectId = "teak-ellipse-444102-d3";
    public final String instanceId = "g23ai2087-instance";
    public final String COLUMN_FAMILY = "sensor";
    public final String tableId = "weather";
    private BigtableDataClient bigtableClient;
    // Clients for Bigtable
    private BigtableDataClient dataClient;
    private BigtableTableAdminClient adminClient;

    // Main method with a menu-driven program
    public static void main(String[] args) throws Exception {
        Bigtable bigtable = new Bigtable();
        Scanner scanner = new Scanner(System.in);
        
        while (true) {
            System.out.println("\n=== Bigtable Menu ===");
            System.out.println("1. Connect to Bigtable");
            System.out.println("2. Create Table");
            System.out.println("3. Delete Table");
            System.out.println("4. Load Data");
            System.out.println("5. Query 1: Temperature at Vancouver");
            System.out.println("6. Query 2: Highest Wind Speed in Portland");
            System.out.println("7. Query 3: All Readings for SeaTac");
            System.out.println("8. Query 4: Highest Temperature in Summer");
            System.out.println("9. Exit");
            System.out.print("Enter your choice: ");
            int choice = scanner.nextInt();

            switch (choice) {
                case 1:
                    bigtable.connect();
                    break;
                case 2:
                    bigtable.createTable();
                    break;
                case 3:
                    bigtable.deleteTable();
                    break;
                case 4:
                    bigtable.loadData();
                    break;
                case 5:
                    System.out.println("Temperature: " + bigtable.query1());
                    break;
                case 6:
                    System.out.println("Highest Wind Speed: " + bigtable.query2());
                    break;
                case 7:
                    ArrayList<Object[]> data = bigtable.query3();
                    System.out.println("Readings for SeaTac:");
                    for (Object[] row : data) {
                        for (Object val : row) {
                            System.out.print(val + " ");
                        }
                        System.out.println();
                    }
                    break;
                case 8:
                    System.out.println("Highest Temperature in Summer: " + bigtable.query4());
                    break;
                case 9:
                    System.out.println("Exiting...");
                    bigtable.close();
                    scanner.close();
                    return;
                default:
                    System.out.println("Invalid choice! Please try again.");
            }
        }
    }

    // Connect method
    // Connect method
    public void connect() throws IOException {
        System.out.println("Establishing connection to Bigtable...");

        // Initialize data client
        BigtableDataSettings dataSettings = BigtableDataSettings.newBuilder()
                .setProjectId(projectId)
                .setInstanceId(instanceId)
                .build();
        dataClient = BigtableDataClient.create(dataSettings);

        // Initialize admin client
        BigtableTableAdminSettings adminSettings = BigtableTableAdminSettings.newBuilder()
                .setProjectId(projectId)
                .setInstanceId(instanceId)
                .build();
        adminClient = BigtableTableAdminClient.create(adminSettings);

        // Initialize Bigtable client
        bigtableClient = BigtableDataClient.create(dataSettings);

        // Check and create table if it doesn't exist
        try {
            if (!adminClient.exists(tableId)) {
                System.out.println("Table '" + tableId + "' not found. Creating a new table...");
                createTable();
                System.out.println("Table '" + tableId + "' created successfully.");
            } else {
                System.out.println("Table '" + tableId + "' already exists.");
            }
        } catch (Exception e) {
            System.err.println("Error during connection setup: " + e.getMessage());
        }

        System.out.println("Connection to Bigtable established successfully.");
    }

    
    public void createTable() {
        try {
            // Define the table schema with a single column family
            CreateTableRequest createTableRequest = CreateTableRequest.of(tableId)
                    .addFamily(COLUMN_FAMILY); // Add the column family
    
            // Create the table
            adminClient.createTable(createTableRequest);
    
            System.out.println("Table " + tableId + " created successfully with column family: " + COLUMN_FAMILY);
        } catch (Exception e) {
            System.err.println("Error creating table: " + e.getMessage());
        }
    }

    // Delete Table
    public void deleteTable() {
        System.out.println("Deleting table: " + tableId);
        try {
            adminClient.deleteTable(tableId);
            System.out.println("Table " + tableId + " deleted successfully.");
        } catch (NotFoundException e) {
            System.err.println("Table does not exist: " + e.getMessage());
        }
    }

    // Load Data
    // Load Data method
    public void loadData() {
        System.out.println("Loading data into table...");
        String[] files = {"data/portland.csv", "data/seatac.csv", "data/vancouver.csv"};

        try {
            for (String file : files) {
                System.out.println("Reading file: " + file);
                BufferedReader br = new BufferedReader(new FileReader(file));
                String line = br.readLine(); // Skip the header line
            
                String fileName = new File(file).getName();
                String city = fileName.substring(0, fileName.lastIndexOf(".csv"));
            
                while ((line = br.readLine()) != null) {
                    String[] values = line.split(",");
            
                    // Extract data columns
                    String date = values[1].trim();
                    String time = values[2].trim();
                    String temperature = values[3].trim();
                    String dewpoint = values[4].trim();
                    String humidity = values[5].trim();
                    String windspeed = values[6].trim();
                    String pressure = values[7].trim();
            
                    // Construct row key
                    String rowKey = date + "_" + time;
            
                    // Insert rows
                    insertRow(rowKey, COLUMN_FAMILY, "temperature", temperature, city);
                    insertRow(rowKey, COLUMN_FAMILY, "dewpoint", dewpoint, city);
                    insertRow(rowKey, COLUMN_FAMILY, "humidity", humidity, city);
                    insertRow(rowKey, COLUMN_FAMILY, "windspeed", windspeed, city);
                    insertRow(rowKey, COLUMN_FAMILY, "pressure", pressure, city);
                    insertRow(rowKey, COLUMN_FAMILY, "city", city, file);
                }
                br.close();
            }
            
            System.out.println("Data loaded successfully.");
        } catch (Exception e) {
            System.err.println("Error loading data: " + e.getMessage());
            e.printStackTrace();
        }
    }


    // Adjusted insertRow method to include filename parameter
        // Insert Row method
    private void insertRow(String rowKey, String family, String column, String value, String file) {
        try {
            // Create a mutation to insert data
            RowMutation rowMutation = RowMutation.create(tableId, rowKey)
                    .setCell(family, column, value);

            // Apply the mutation
            bigtableClient.mutateRow(rowMutation);

            System.out.println("Inserted row: RowKey='" + rowKey + "', Column='" + family + ":" + column + "', Value='" + value + "'"+ ", File: "+ file);
        } catch (Exception e) {
            System.err.println("Error inserting row: RowKey='" + rowKey + "', Column='" + family + ":" + column + "', Value='" + value + "'");
            e.printStackTrace();
        }
    }
   

    // Query 1
    public int query1() {
        System.out.println("Executing Query 1: Temperature at Vancouver.");
        // Add query logic here
        return 0;
    }

    // Query 2
    public int query2() {
        System.out.println("Executing Query 2: Highest Wind Speed in Portland.");
        // Add query logic here
        return 0;
    }

    // Query 3
    public ArrayList<Object[]> query3() {
        System.out.println("Executing Query 3: Readings for SeaTac.");
        ArrayList<Object[]> data = new ArrayList<>();
        // Add query logic here
        return data;
    }

    // Query 4
    public int query4() {
        System.out.println("Executing Query 4: Highest Temperature in Summer.");
        // Add query logic here
        return 0;
    }

    // Close Bigtable clients
    public void close() {
        if (dataClient != null) {
            dataClient.close();
        }
        if (adminClient != null) {
            adminClient.close();
        }
        System.out.println("Closed Bigtable connections.");
    }
}
