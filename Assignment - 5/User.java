import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;

import redis.clients.jedis.ScanResult;
import redis.clients.jedis.ScanParams;

public class RedisClient {
    private Jedis jedis;

    public RedisClient() {
        try {
            jedis = new Jedis("redis-16150.c16.us-east-1-3.ec2.redns.redis-cloud.com", 16150);
            String password = "OahVB0ixFG49ILZK5qvC6QG6BmujVgHC"; // Replace with your Redis password
            jedis.auth(password);
            System.out.println("Connected to Redis with authentication.");
        } catch (JedisException e) {
            e.printStackTrace();
        }
    }

    public void loadUsers(String filePath) {
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue; // Skip empty lines
                String[] parts = line.split(" (?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"); // Split while preserving quoted text
                if (parts.length < 2) {
                    System.out.println("Skipping malformed line: " + line);
                    continue; // Skip lines that don't match the expected format
                }
                String key = parts[0].replace("\"", ""); // Remove quotes from the key
                Map<String, String> user = new HashMap<>();
                for (int i = 1; i < parts.length; i += 2) {
                    if (i + 1 < parts.length) {
                        String attributeKey = parts[i].replace("\"", "");
                        String attributeValue = parts[i + 1].replace("\"", "");
                        user.put(attributeKey, attributeValue);
                    } else {
                        System.out.println("Skipping incomplete key-value pair in line: " + line);
                    }
                }
                jedis.hset(key, user);
            }
            System.out.println("Users loaded.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    
    public void loadScores(String filename) {
        try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
            String line;
            boolean firstLine = true; // To skip the header line
            while ((line = br.readLine()) != null) {
                // Skip the header row
                if (firstLine) {
                    firstLine = false;
                    continue;
                }
                String[] values = line.split(",");
                String userId = values[0].trim(); // User ID
                try {
                    double score = Double.parseDouble(values[1].trim()); // Parse the score
                    int leaderboard = Integer.parseInt(values[2].trim()); // Parse the leaderboard position
    
                    // Add the score to the user
                    users.put(userId, new User(userId, score, leaderboard)); 
                } catch (NumberFormatException e) {
                    System.out.println("Error parsing score or leaderboard for " + userId);
                    e.printStackTrace();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    

    public Map<String, String> query1(String userId) {
        return jedis.hgetAll(userId);
    }

    public String query2(String userId) {
        String latitude = jedis.hget(userId, "latitude");
        String longitude = jedis.hget(userId, "longitude");
        return "Latitude: " + latitude + ", Longitude: " + longitude;
    }

    public Map<String, String> query3() {
        Map<String, String> result = new HashMap<>();
        String cursor = "0";
        do {
            // Use SCAN instead of KEYS
            ScanResult<String> scanResult = jedis.scan(cursor, new ScanParams().match("user:*"));
            cursor = scanResult.getCursor(); // Update cursor for next scan
            for (String key : scanResult.getResult()) {
                if (Character.getNumericValue(key.charAt(5)) % 2 == 0) {
                    result.put(key, jedis.hget(key, "last_name"));
                }
            }
        } while (!"0".equals(cursor)); // Continue scanning until cursor is "0" (end of data)
        return result;
    }
    
    public List<String> query4() {
        List<String> result = new ArrayList<>();
        String cursor = "0";
        do {
            // Use SCAN instead of KEYS
            ScanResult<String> scanResult = jedis.scan(cursor, new ScanParams().match("user:*"));
            cursor = scanResult.getCursor(); // Update cursor for next scan
            for (String key : scanResult.getResult()) {
                String gender = jedis.hget(key, "gender");
                String country = jedis.hget(key, "country");
                double latitude = Double.parseDouble(jedis.hget(key, "latitude"));
                if ("female".equalsIgnoreCase(gender) &&
                    ("China".equalsIgnoreCase(country) || "Russia".equalsIgnoreCase(country)) &&
                    (latitude >= 40 && latitude <= 46)) {
                    result.add(key);
                }
            }
        } while (!"0".equals(cursor)); // Continue scanning until cursor is "0" (end of data)
        return result;
    }

    public void query5() {
        Set<String> topPlayers = jedis.zrevrange("leaderboard:2", 0, 9);

        for (String playerName : topPlayers) {
            double score = jedis.zscore("leaderboard:2", playerName);
            String email = jedis.hget(playerName, "email");
            System.out.println("Player: " + playerName + " | Score: " + score + " | Email: " + email);
        }
    }

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        RedisClient client = new RedisClient();
        
        boolean running = true;
        while (running) {
            System.out.println("\n===== Redis Menu =====");
            System.out.println("1. Load Users from file");
            System.out.println("2. Load Scores from file");
            System.out.println("3. Query 1 (Get user info)");
            System.out.println("4. Query 2 (Get latitude and longitude)");
            System.out.println("5. Query 3 (Get even user IDs)");
            System.out.println("6. Query 4 (Get female users from China/Russia with specific latitude)");
            System.out.println("7. Query 5 (Show top players and scores)");
            System.out.println("8. Exit");
            System.out.print("Enter your choice: ");

            int choice = scanner.nextInt();
            scanner.nextLine();  // Consume the newline

            switch (choice) {
                case 1:
                    System.out.print("Enter the file path for users: ");
                    String userFile = scanner.nextLine();
                    client.loadUsers(userFile);
                    break;
                case 2:
                    System.out.print("Enter the file path for scores: ");
                    String scoreFile = scanner.nextLine();
                    client.loadScores(scoreFile);
                    break;
                case 3:
                    System.out.print("Enter user ID: ");
                    String userId1 = scanner.nextLine();
                    System.out.println(client.query1(userId1));
                    break;
                case 4:
                    System.out.print("Enter user ID: ");
                    String userId2 = scanner.nextLine();
                    System.out.println(client.query2(userId2));
                    break;
                case 5:
                    System.out.println(client.query3());
                    break;
                case 6:
                    System.out.println(client.query4());
                    break;
                case 7:
                    client.query5();
                    break;
                case 8:
                    System.out.println("Exiting...");
                    running = false;
                    break;
                default:
                    System.out.println("Invalid choice, please try again.");
            }
        }
        scanner.close();
    }
}

public class User {
    private String userId;
    private double score;
    private int leaderboard;

    // Constructor
    public User(String userId, double score, int leaderboard) {
        this.userId = userId;
        this.score = score;
        this.leaderboard = leaderboard;
    }

    // Getter methods
    public String getUserId() {
        return userId;
    }

    public double getScore() {
        return score;
    }

    public int getLeaderboard() {
        return leaderboard;
    }

    // Optional: Override toString() for better printing
    @Override
    public String toString() {
        return "User{id='" + userId + "', score=" + score + ", leaderboard=" + leaderboard + "}";
    }
}

