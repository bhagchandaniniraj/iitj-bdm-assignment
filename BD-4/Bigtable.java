import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.data.v2.models.Query;

import java.util.Scanner;

public class Bigtable {
    private BigtableDataClient dataClient;
    private final String projectId = "your-project-id";
    private final String instanceId = "your-instance-id";
    private final String tableId = "your-table-id";

    // Connect to Bigtable
    public void connect() throws Exception {
        dataClient = BigtableDataClient.create(projectId, instanceId);
        System.out.println("Connected to Bigtable.");
    }

    // Insert sample data
    public void loadData() throws Exception {
        System.out.println("Inserting sample data...");
        String rowKey = "sample-row";
        RowMutation mutation = RowMutation.create(tableId, rowKey)
                .setCell("cf1", "column1", "value1");
        dataClient.mutateRow(mutation);
        System.out.println("Sample data loaded successfully.");
    }

    // Query data
    public String query1() throws Exception {
        Query query = Query.create(tableId)
                .filter(Filters.key().exactMatch("sample-row"));

        Row row = dataClient.readRow(query);
        if (row != null) {
            return row.toString();
        }
        return "No data found.";
    }

    // Menu-driven method
    public void run() throws Exception {
        connect();
        loadData();

        Scanner scanner = new Scanner(System.in);
        boolean exit = false;

        while (!exit) {
            System.out.println("\nChoose an operation:");
            System.out.println("1. Query data (Query1)");
            System.out.println("2. Exit");
            System.out.print("Enter your choice: ");

            int choice = scanner.nextInt();
            switch (choice) {
                case 1:
                    System.out.println("Query1 result: " + query1());
                    break;
                case 2:
                    exit = true;
                    System.out.println("Exiting...");
                    break;
                default:
                    System.out.println("Invalid choice. Please try again.");
            }
        }

        scanner.close();
        dataClient.close();
        System.out.println("Disconnected from Bigtable.");
    }

    public static void main(String[] args) {
        Bigtable bigtable = new Bigtable();
        try {
            bigtable.run();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
