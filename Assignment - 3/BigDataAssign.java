

import java.sql.*;

class BigDataAssign{
    private Connection connection;
    //private String url = "jdbc:mysql://db-g23ai2087.cojx0qdcaqlm.us-east-1.rds.amazonaws.com/db_g23ai2087";
    private String url = "jdbc:mysql://localhost/db_g23ai2087";
    // private String user = "admin";
    // private String password = "IITJ1122";
    private String user = "root";
    private String password = "";
    private String db = "db_g23ai2087";
    private Statement stmt;
    private ResultSet rs;

    public void resultSetMetaDataToString(String table){
        String query = "SELECT table_schema, table_name, column_name, ordinal_position, data_type,numeric_precision, column_type, column_default, is_nullable, column_comment FROM information_schema.columns WHERE table_name = '"+table+"' order by ordinal_position";
        try{
            ResultSet rs = this.executeMe(query,"Query");
            this.print(rs,"Metadata: "+table);
        }catch(Exception e){
            System.out.println("Error: Generating Metadata - " +e.getMessage());
        }
    }
    public void queryOne(){
        String query = """
            SELECT name, annualRevenue, numEmployees
            FROM company
            WHERE numEmployees > 10000 OR annualRevenue < 1000000
            ORDER BY name ASC
            """;
        try{
            this.rs = this.executeMe(query,"Query");
            this.print(rs, "Query One");
        }catch(Exception e){
            System.out.println("Error Executing Query: " + e.getMessage());
        }
    }
    public void queryTwo(){
        String query = """
            SELECT c.name, c.ticker, 
                   MIN(s.lowPrice) AS lowestPrice, 
                   MAX(s.highPrice) AS highestPrice, 
                   AVG(s.closePrice) AS avgClosingPrice, 
                   AVG(s.volume) AS avgVolume
            FROM company c
            JOIN stockprice s ON c.id = s.companyId
            WHERE s.priceDate BETWEEN '2022-08-22' AND '2022-08-26'
            GROUP BY c.id, c.name, c.ticker
            ORDER BY avgVolume DESC
            """;
        try{
            this.rs = this.executeMe(query,"Query");
            this.print(rs, "Query Two");
        }catch(Exception e){
            System.out.println("Error Executing Query: " + e.getMessage());
        }
    }
    public void queryThree(){
        String query = """
        SELECT c.name, c.ticker, s.closePrice
        FROM company c
        LEFT JOIN stockprice s ON c.id = s.companyId
        AND s.priceDate = '2022-08-30'
        WHERE (s.closePrice IS NULL OR s.closePrice >= 0.9 * ( 
                    SELECT AVG(closePrice) 
                    FROM stockprice 
                    WHERE priceDate BETWEEN '2022-08-15' AND '2022-08-19' 
                ))
        ORDER BY c.name ASC;
        """;
        try{
            this.rs = this.executeMe(query,"Query");
            this.print(rs, "Query Two");
        }catch(Exception e){
            System.out.println("Error Executing Query: " + e.getMessage());
        }
    }
    public void createDatabase(){
        String sql = "CREATE DATABASE IF NOT EXISTS db_g23ai2087";
        this.executeMe(sql, "Update");
        System.out.println("createDatabase() executed successfully");
    }

    public ResultSet executeMe(String query, String flag){
        try{
            if(flag == "Update"){
                int status = this.stmt.executeUpdate(query);
            }else if(flag == "Query"){
                this.rs = this.stmt.executeQuery(query);
            }
        }catch (SQLException e) {
            System.out.println("Error Executing Query: " + e.getMessage());
        }
        return this.rs;
    }
    public void createTable(){
        String createCompany = """
                CREATE TABLE company (
                    id INT PRIMARY KEY,
                    name VARCHAR(50),
                    ticker CHAR(10),
                    annualRevenue DECIMAL(15, 2),
                    numEmployees INT
                )
                """; 

            String createStockPrice = """
                CREATE TABLE stockprice (
                    companyId INT,
                    priceDate DATE,
                    openPrice DECIMAL(10, 2),
                    highPrice DECIMAL(10, 2),
                    lowPrice DECIMAL(10, 2),
                    closePrice DECIMAL(10, 2),
                    volume INT,
                    PRIMARY KEY (companyId, priceDate),
                    FOREIGN KEY (companyId) REFERENCES company(id) ON DELETE CASCADE
                )
                """;
            try{
                this.executeMe(createCompany,"Update");
                this.executeMe(createStockPrice,"Update");
                System.out.println("\nTable Created Successfully");
            }catch(Exception e){
                System.out.println("Error: Creating Database - " +e.getMessage());
            }
    }

    public void dropTables(){
        try{
            String query = "DROP TABLE stockprice";
            this.executeMe(query,"Update");
            query = "DROP TABLE company";
            this.executeMe(query,"Update");
            System.out.println("Tables Deleted Successfully");
        }catch(Exception e){
            System.out.println("Error: Creating Database - " +e.getMessage());
        }
    }
    public void print(ResultSet rs, String table){
        int count = 0;
        String str = "";
        try{
                str += "\n================================================================\n";
                ResultSetMetaData metaData = rs.getMetaData();
                int columnCount = metaData.getColumnCount();
                str += table + ": Columns Found: " +columnCount;
                str += "\n================================================================\n";
                for(int i = 1; i <= columnCount; i++){
                    str += metaData.getColumnName(i);
                    if(i == columnCount) continue;
                    str += ",";
                }
                str += "\n================================================================\n";
                while(rs.next()){
                    for(int i = 1; i <= columnCount; i++){
                        str += rs.getString(i);
                        if(i == columnCount) continue;
                        str += ",";
                    }
                    count++;
                    str +="\n";
                }
            }catch(Exception e){
                System.out.println("Error: Result Set to String Conversion: - " +e.getMessage());
            }
            str += "================================================================\n";
            str += "Records found in table: "+count+"\n";
            str += "================================================================\n";
            System.out.println(str);
    }
    public void resultSetToString(String table){
        String query = "SELECT * FROM "+table;
        try{
            ResultSet rs = this.executeMe(query,"Query");
            this.print(rs,"Table - "+table);
        }catch(Exception e){
            System.out.println("Error: Result Set to String Conversion: - " +e.getMessage());
        }
    }
    public void dropDB(){
        try{
            String query = "DROP database "+ this.db;
            this.executeMe(query,"Update");
            System.out.println("Database Dropped Successfully");
        }catch(Exception e){
            System.out.println("Error: Creating Database - " +e.getMessage());
        }
    }
    public void insertData(){
        String company = """
            INSERT INTO company (id,name,ticker,annualRevenue,numEmployees)
            VALUES 
            (1, 'Apple', 'AAPL', 387540000000.00 , 154000),
            (2, 'GameStop', 'GME', 611000000.00, 12000),
            (3, 'Handy Repair', null, 2000000, 50),
            (4, 'Microsoft', 'MSFT', '198270000000.00' , 221000),
            (5, 'StartUp', null, 50000, 3)
        """;
        String stockprices ="""
                INSERT INTO stockprice 
                (companyId,priceDate,openPrice,highPrice,lowPrice,closePrice,volume)
                VALUES (1, '2022-08-15', 171.52, 173.39, 171.35, 173.19, 54091700),
                (1, '2022-08-16', 172.78, 173.71, 171.66, 173.03, 56377100),
                (1, '2022-08-17', 172.77, 176.15, 172.57, 174.55, 79542000),
                (1, '2022-08-18', 173.75, 174.90, 173.12, 174.15, 62290100),
                (1, '2022-08-19', 173.03, 173.74, 171.31, 171.52, 70211500),
                (1, '2022-08-22', 169.69, 169.86, 167.14, 167.57, 69026800),
                (1, '2022-08-23', 167.08, 168.71, 166.65, 167.23, 54147100),
                (1, '2022-08-24', 167.32, 168.11, 166.25, 167.53, 53841500),
                (1, '2022-08-25', 168.78, 170.14, 168.35, 170.03, 51218200),
                (1, '2022-08-26', 170.57, 171.05, 163.56, 163.62, 78823500),
                (1, '2022-08-29', 161.15, 162.90, 159.82, 161.38, 73314000),
                (1, '2022-08-30', 162.13, 162.56, 157.72, 158.91, 77906200),
                (2, '2022-08-15', 39.75, 40.39, 38.81, 39.68, 5243100),
                (2, '2022-08-16', 39.17, 45.53, 38.60, 42.19, 23602800),
                (2, '2022-08-17', 42.18, 44.36, 40.41, 40.52, 9766400),
                (2, '2022-08-18', 39.27, 40.07, 37.34, 37.93, 8145400),
                (2, '2022-08-19', 35.18, 37.19, 34.67, 36.49, 9525600),
                (2, '2022-08-22', 34.31, 36.20, 34.20, 34.50, 5798600),
                (2, '2022-08-23', 34.70, 34.99, 33.45, 33.53, 4836300),
                (2, '2022-08-24', 34.00, 34.94, 32.44, 32.50, 5620300),
                (2, '2022-08-25', 32.84, 32.89, 31.50, 31.96, 4726300),
                (2, '2022-08-26', 31.50, 32.38, 30.63, 30.94, 4289500),
                (2, '2022-08-29', 30.48, 32.75, 30.38, 31.55, 4292700),
                (2, '2022-08-30', 31.62, 31.87, 29.42, 29.84, 5060200),
                (4, '2022-08-15', 291.00, 294.18, 290.11, 293.47, 18085700),
                (4, '2022-08-16', 291.99, 294.04, 290.42, 292.71, 18102900),
                (4, '2022-08-17', 289.74, 293.35, 289.47, 291.32, 18253400),
                (4, '2022-08-18', 290.19, 291.91, 289.08, 290.17, 17186200),
                (4, '2022-08-19', 288.90, 289.25, 285.56, 286.15, 20557200),
                (4, '2022-08-22', 282.08, 282.46, 277.22, 277.75, 25061100),
                (4, '2022-08-23', 276.44, 278.86, 275.40, 276.44, 17527400),
                (4, '2022-08-24', 275.41, 277.23, 275.11, 275.79, 18137000),
                (4, '2022-08-25', 277.33, 279.02, 274.52, 278.85, 16583400),
                (4, '2022-08-26', 279.08, 280.34, 267.98, 268.09, 27532500),
                (4, '2022-08-29', 265.85, 267.40, 263.85, 265.23, 20338500),
                (4, '2022-08-30', 266.67, 267.05, 260.66, 262.97, 22767100)
        """;
        try{
            this.executeMe(company,"Update");
            this.executeMe(stockprices,"Update");
        }catch(Exception e){
            System.out.println("Error: Inserting Data: "+e.getMessage());
        }
    }
    public void connect(){
        try {
            Class.forName("com.mysql.jdbc.Driver");
            connection = DriverManager.getConnection(this.url, this.user, this.password);
            System.out.println("Connected to the Server.");
            this.stmt = connection.createStatement();
        } catch (Exception e) {
            System.out.println("Error connecting to the database: " + e.getMessage());
        }
    }
    public Connection getConnection(){
        return this.connection;
    }
    public Statement getStatement(){
        return this.stmt;
    }

    public void disconnect() {
        try {
            if (this.connection != null && !this.connection.isClosed()) {
                this.connection.close();
                System.out.println("Disconnected from the database.");
            }
        } catch (SQLException e) {
            System.out.println("Error disconnecting from the database: " + e.getMessage());
        }
    }
}
