import redis.clients.jedis.*;
import redis.clients.jedis.Tuple;

import java.util.*;

public class RedisClient {
    private Jedis jedis;

    public RedisClient() {
        this.jedis = new Jedis("redis-16150.c16.us-east-1-3.ec2.redns.redis-cloud.com", 16150);
    }

    // Connect to Redis
    public void connect() {
        try {
            System.out.println("Connecting to Redis...");
            jedis.connect();
            System.out.println("Connected to Redis!");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Load Users Data into Redis DB
    public void loadUsers(String filePath) {
        try {
            System.out.println("Loading user data...");
            // Code to load data from file into Redis
            // Example of loading data:
            jedis.set("user:1:name", "John");
            jedis.set("user:2:name", "Alice");
            // This is a simple example, adjust it according to your dataset format
            System.out.println("User data loaded successfully.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Load Scores Data into Redis DB
    public void loadScores() {
        try {
            System.out.println("Loading scores data...");
            // Example of loading scores into Redis
            jedis.zadd("leaderboard:2", 100, "player1");
            jedis.zadd("leaderboard:2", 200, "player2");
            // Adjust this according to your dataset format
            System.out.println("Scores data loaded successfully.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Query 1: Return all attributes of the user by usr
    public void query1(int usr) {
        System.out.println("Executing Query 1: Fetching all attributes for user " + usr);
        String name = jedis.get("user:" + usr + ":name");
        // Fetch other attributes similarly
        System.out.println("User details: Name = " + name);
    }

    // Query 2: Return the coordinate (longitude and latitude) of the user by usr
    public void query2(int usr) {
        System.out.println("Executing Query 2: Fetching coordinates for user " + usr);
        String longitude = jedis.get("user:" + usr + ":longitude");
        String latitude = jedis.get("user:" + usr + ":latitude");
        System.out.println("Coordinates for user " + usr + ": Longitude = " + longitude + ", Latitude = " + latitude);
    }

    // Query 3: Get the keys and last names of the users whose ids do not start with an odd number
    public void query3() {
        System.out.println("Executing Query 3: Fetching users whose IDs do not start with an odd number");
        Set<String> keys = jedis.keys("user:*:name");
        for (String key : keys) {
            String userId = key.split(":")[1];
            if (Integer.parseInt(userId) % 2 == 0) {
                String lastName = jedis.get(key);
                System.out.println("User ID: " + userId + ", Last Name: " + lastName);
            }
        }
    }

    // Query 4: Return the female in China or Russia with a latitude between 40 and 46
    public void query4() {
        System.out.println("Executing Query 4: Fetching female users in China or Russia with latitude between 40 and 46");
        Set<String> users = jedis.keys("user:*:gender");
        for (String user : users) {
            String gender = jedis.get(user);
            String country = jedis.get(user.replace("gender", "country"));
            String latitude = jedis.get(user.replace("gender", "latitude"));
            if (gender.equals("female") && (country.equals("China") || country.equals("Russia")) &&
                Double.parseDouble(latitude) >= 40 && Double.parseDouble(latitude) <= 46) {
                System.out.println("User: " + user + " is a female from " + country + " with latitude " + latitude);
            }
        }
    }

    // Query 5: Get the email ids of the top 10 players in leaderboard:2
    public void query5() {
        System.out.println("Executing Query 5: Fetching top 10 players by score in leaderboard:2");
        Set<Tuple> topPlayers = jedis.zrevrangeWithScores("leaderboard:2", 0, 9);
        for (Tuple player : topPlayers) {
            String playerName = player.getElement();
            String email = jedis.get("user:" + playerName + ":email");
            System.out.println("Player: " + playerName + " Email: " + email);
        }
    }

    // Main method for menu-driven execution
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        RedisClient redisClient = new RedisClient();
        redisClient.connect();

        boolean exit = false;
        while (!exit) {
            System.out.println("\nMenu:");
            System.out.println("1. Load Users Data");
            System.out.println("2. Load Scores Data");
            System.out.println("3. Query 1: Get User Details");
            System.out.println("4. Query 2: Get Coordinates of User");
            System.out.println("5. Query 3: Get Users with Even IDs");
            System.out.println("6. Query 4: Get Female Users from China/Russia with Latitude Between 40 and 46");
            System.out.println("7. Query 5: Get Top 10 Players by Score");
            System.out.println("8. Exit");
            System.out.print("Choose an option: ");
            int choice = scanner.nextInt();
            scanner.nextLine();  // Consume newline

            switch (choice) {
                case 1:
                    System.out.print("Enter file path for users data: ");
                    String userFilePath = scanner.nextLine();
                    redisClient.loadUsers(userFilePath);
                    break;
                case 2:
                    redisClient.loadScores();
                    break;
                case 3:
                    System.out.print("Enter user ID for query 1: ");
                    int usr1 = scanner.nextInt();
                    redisClient.query1(usr1);
                    break;
                case 4:
                    System.out.print("Enter user ID for query 2: ");
                    int usr2 = scanner.nextInt();
                    redisClient.query2(usr2);
                    break;
                case 5:
                    redisClient.query3();
                    break;
                case 6:
                    redisClient.query4();
                    break;
                case 7:
                    redisClient.query5();
                    break;
                case 8:
                    exit = true;
                    System.out.println("Exiting...");
                    break;
                default:
                    System.out.println("Invalid choice, please try again.");
            }
        }
        scanner.close();
    }
}
