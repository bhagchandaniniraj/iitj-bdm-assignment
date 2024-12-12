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
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.RowCell;


import java.util.ArrayList;
import java.util.Scanner;
import java.io.*;
import java.util.List;


public class Bigtable {
    public final String projectId = "teak-ellipse-444102-d3";
    public final String instanceId = "g23ai2087-instance";
    public final String COLUMN_FAMILY = "sensor";
    public final String tableId = "weather";
    private BigtableDataClient bigtableClient;

    private BigtableDataClient dataClient;
    private BigtableTableAdminClient adminClient;
    private static final String TEMPERATURE_COLUMN = "temperature";
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
                    System.out.println("Highest Wind Speed: " + bigtable.query2(bigtable.tableId,"Portland"));
                    break;
                case 7:
                    List<String> readings = bigtable.query3();
                    for (String reading : readings) {
                        System.out.println(reading);
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

        bigtableClient = BigtableDataClient.create(dataSettings);

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
            CreateTableRequest createTableRequest = CreateTableRequest.of(tableId)
                    .addFamily(COLUMN_FAMILY); 
    
            // Create the table
            adminClient.createTable(createTableRequest);
    
            System.out.println("Table " + tableId + " created successfully with column family: " + COLUMN_FAMILY);
        } catch (Exception e) {
            System.err.println("Error creating table: " + e.getMessage());
        }
    }

    public void deleteTable() {
        System.out.println("Deleting table: " + tableId);
        try {
            adminClient.deleteTable(tableId);
            System.out.println("Table " + tableId + " deleted successfully.");
        } catch (NotFoundException e) {
            System.err.println("Table does not exist: " + e.getMessage());
        }
    }

    public void loadData() {
        System.out.println("Loading data into table...");
        String[] files = {"data/portland.csv", "data/seatac.csv", "data/vancouver.csv"};
        int batchSize = 500; 
        int count = 1;

        try {
            for (String file : files) {
                System.out.println("Reading file: " + file);
                BufferedReader br = new BufferedReader(new FileReader(file));
                String line = br.readLine(); 
                int cnt = 1;
                String fileName = new File(file).getName();
                String city = fileName.substring(0, fileName.lastIndexOf(".csv"));

                List<RowMutation> rowMutations = new ArrayList<>();

                while ((line = br.readLine()) != null) {
                    String[] values = line.split(",");
                    String date = values[1].trim();
                    String time = values[2].trim();
                    String temperature = values[3].trim();
                    String dewpoint = values[4].trim();
                    String humidity = values[5].trim();
                    String windspeed = values[6].trim();
                    String pressure = values[7].trim();

                    String rowKey = date + "_" + time;
                    rowMutations.add(RowMutation.create(tableId, rowKey)
                            .setCell(COLUMN_FAMILY, "temperature", temperature)
                            .setCell(COLUMN_FAMILY, "dewpoint", dewpoint)
                            .setCell(COLUMN_FAMILY, "humidity", humidity)
                            .setCell(COLUMN_FAMILY, "windspeed", windspeed)
                            .setCell(COLUMN_FAMILY, "pressure", pressure)
                            .setCell(COLUMN_FAMILY, "city", city));
                    System.out.println(cnt++ +")City: "+city+", Temp:" +temperature+", Dew:"+dewpoint+", humidity:"+humidity+", ws:"+windspeed+", press:"+pressure);

                    if (rowMutations.size() == batchSize) {
                        batchInsert(rowMutations);
                        rowMutations.clear();
                        System.out.println("Batch of " + batchSize + " rows inserted.");
                    }

                    System.out.println("Row: " + ++count + " Prepared!");
                }

                if (!rowMutations.isEmpty()) {
                    batchInsert(rowMutations);
                    System.out.println("Final batch for " + city + " inserted.");
                }
                br.close();
            }

            System.out.println("Data loaded successfully.");
        } catch (Exception e) {
            System.err.println("Error loading data: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void batchInsert(List<RowMutation> rowMutations) {
        for (RowMutation mutation : rowMutations) {
            try {
                bigtableClient.mutateRow(mutation);
            } catch (Exception e) {
                System.err.println("Error inserting batch: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }
    
    public String query1() {
        String rowKey = "2022-10-01_10:00";

        try {
            Row row = bigtableClient.readRow(tableId, rowKey);
            if (row != null && row.getCells(COLUMN_FAMILY, TEMPERATURE_COLUMN).size() > 0) {
                // Get the latest value for the temperature column
                return row.getCells(COLUMN_FAMILY, TEMPERATURE_COLUMN).get(0).getValue().toStringUtf8();
            } else {
                return "Temperature data not found for the given row key.";
            }
        } catch (Exception e) {
            System.err.println("Error querying Bigtable: " + e.getMessage());
            e.printStackTrace();
            return "Error occurred while querying.";
        }
    }

    public String query2(String tableId, String city) {
        try {
            String rowKeyPrefix = city + "_2022-09";
            String highestWindSpeedRowKey = null;
            double maxWindSpeed = Double.MIN_VALUE;
            Query query = Query.create(tableId).prefix(rowKeyPrefix);
    
            for (Row row : bigtableClient.readRows(query)) {
                // Get windspeed cells
                List<RowCell> windspeedCells = row.getCells(COLUMN_FAMILY, "windspeed");
                for (RowCell cell : windspeedCells) {
                    String windSpeedValue = cell.getValue().toStringUtf8();
                    double windSpeed = Double.parseDouble(windSpeedValue);
    
                    // Update max wind speed if current value is higher
                    if (windSpeed > maxWindSpeed) {
                        maxWindSpeed = windSpeed;
                        highestWindSpeedRowKey = row.getKey().toStringUtf8();
                    }
                }
            }
    
            if (highestWindSpeedRowKey != null) {
                return "Highest wind speed in September 2022 in Portland: " + maxWindSpeed + " km/h";
            } else {
                return "No data found for the specified query.";
            }
        } catch (Exception e) {
            e.printStackTrace();
            return "Error occurred while querying data.";
        }
    }
    
    public List<String> query3() {
        System.out.println("Executing Query 3: Retrieving readings for SeaTac on October 1, 2022, at 10:00.");
    
        String tableId = this.tableId; 
        String rowKey = "2022-10-01_10:00";
    
        List<String> readings = new ArrayList<>();
        try {
            Row row = bigtableClient.readRow(tableId, rowKey);
    
            if (row != null) {
                StringBuilder rowData = new StringBuilder();
                rowData.append("RowKey: ").append(row.getKey().toStringUtf8()).append(", Data: {");
    
                // Iterate over all cells in the row
                for (RowCell cell : row.getCells()) {
                    String columnQualifier = cell.getQualifier().toStringUtf8();
                    String value = cell.getValue().toStringUtf8();
                    rowData.append(columnQualifier).append(": ").append(value).append(", ");
                }
    
                if (rowData.lastIndexOf(", ") > 0) {
                    rowData.delete(rowData.lastIndexOf(", "), rowData.length());
                }
                rowData.append("}");
    
                readings.add(rowData.toString());
            } else {
                readings.add("No data found for row key: " + rowKey);
            }
        } catch (Exception e) {
            e.printStackTrace();
            readings.add("Error occurred while querying data.");
        }
        return readings;
    }
    
    // Query 4
    public int query4() {
        System.out.println("Executing Query 4: Highest Temperature in Summer 2022 (July, August).");
        String tableId = "weather"; // Replace with your Bigtable table ID

        String[] summerMonths = {"2022-07", "2022-08"};
        int highestTemperature = Integer.MIN_VALUE; 
        try {
            for (String month : summerMonths) {
                // Construct the row key prefix for each month
                String rowKeyPrefix = month;
    
                Query query = Query.create(tableId).prefix(rowKeyPrefix);
    
                for (Row row : bigtableClient.readRows(query)) {
                    // Iterate over all cells in the row to find the temperature column
                    for (RowCell cell : row.getCells(COLUMN_FAMILY, "temperature")) {
                        try {
                            int temp = Integer.parseInt(cell.getValue().toStringUtf8());
                            if (temp > highestTemperature) {
                                highestTemperature = temp; // Update the highest temperature
                            }
                        } catch (NumberFormatException e) {
                            System.err.println("Invalid temperature value: " + cell.getValue().toStringUtf8());
                        }
                    }
                }
            }
    
            if (highestTemperature == Integer.MIN_VALUE) {
                System.out.println("No temperature data found for summer months.");
            } else {
                System.out.println("Highest Temperature in Summer 2022: " + highestTemperature);
            }
    
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error occurred while querying data.");
        }
    
        return highestTemperature;
    }

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
