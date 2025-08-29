package Bus;

import java.sql.*;

public class Main {
    public static final String JDBC_URL = "jdbc:sqlite:company.db";

    public static void main(String[] args) throws Exception {
        try { Class.forName("org.sqlite.JDBC"); } catch (ClassNotFoundException ignored) {}

        try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
            conn.setAutoCommit(false);
            try {
                createSchemaAndSeedData(conn);
                queryAndPrintResult(conn);
                conn.commit();
            } catch (Exception e) {
                conn.rollback();
                throw e;
            }
        }
    }

    private static void createSchemaAndSeedData(Connection conn) throws SQLException {
        String[] sqls = {
            "DROP TABLE IF EXISTS PAYMENTS;",
            "DROP TABLE IF EXISTS EMPLOYEE;",
            "DROP TABLE IF EXISTS DEPARTMENT;",

            "CREATE TABLE DEPARTMENT (DEPARTMENT_ID INTEGER PRIMARY KEY, DEPARTMENT_NAME TEXT NOT NULL);",
            "CREATE TABLE EMPLOYEE (EMP_ID INTEGER PRIMARY KEY, FIRST_NAME TEXT NOT NULL, LAST_NAME TEXT NOT NULL, DOB TEXT NOT NULL, GENDER TEXT NOT NULL, DEPARTMENT INTEGER NOT NULL, FOREIGN KEY(DEPARTMENT) REFERENCES DEPARTMENT(DEPARTMENT_ID));",
            "CREATE TABLE PAYMENTS (PAYMENT_ID INTEGER PRIMARY KEY, EMP_ID INTEGER NOT NULL, AMOUNT REAL NOT NULL, PAYMENT_TIME TEXT NOT NULL, FOREIGN KEY(EMP_ID) REFERENCES EMPLOYEE(EMP_ID));",

            "INSERT INTO DEPARTMENT VALUES (1,'HR'), (2,'Finance'), (3,'Engineering'), (4,'Sales'), (5,'Marketing'), (6,'IT');",

            "INSERT INTO EMPLOYEE VALUES " +
            "(1,'John','Williams','1980-05-15','Male',3)," +
            "(2,'Sarah','Johnson','1990-07-20','Female',2)," +
            "(3,'Michael','Smith','1985-02-10','Male',3)," +
            "(4,'Emily','Brown','1992-11-30','Female',4)," +
            "(5,'David','Jones','1988-09-05','Male',5)," +
            "(6,'Olivia','Davis','1995-04-12','Female',1)," +
            "(7,'James','Wilson','1983-03-25','Male',6)," +
            "(8,'Sophia','Anderson','1991-08-17','Female',4)," +
            "(9,'Liam','Miller','1979-12-01','Male',1)," +
            "(10,'Emma','Taylor','1993-06-28','Female',5);",

            "INSERT INTO PAYMENTS VALUES " +
            "(1,2,65784.00,'2025-01-01 13:44:12.824')," +
            "(2,4,62736.00,'2025-01-06 18:36:37.892')," +
            "(3,1,69437.00,'2025-01-01 10:19:21.563')," +
            "(4,3,67183.00,'2025-01-02 17:21:57.341')," +
            "(5,2,66273.00,'2025-02-01 11:49:15.764')," +
            "(6,5,71475.00,'2025-01-01 07:24:14.453')," +
            "(7,1,70837.00,'2025-02-03 19:11:31.553')," +
            "(8,6,69628.00,'2025-01-02 10:41:15.113')," +
            "(9,4,71876.00,'2025-02-01 12:16:47.807')," +
            "(10,3,70098.00,'2025-02-03 10:11:17.341')," +
            "(11,6,67827.00,'2025-02-02 19:21:27.753')," +
            "(12,5,69871.00,'2025-02-05 17:54:17.453')," +
            "(13,2,72984.00,'2025-03-05 09:37:35.974')," +
            "(14,1,67982.00,'2025-03-01 06:09:51.983')," +
            "(15,6,70198.00,'2025-03-02 10:34:35.753')," +
            "(16,4,74998.00,'2025-03-02 09:27:26.162');"
        };

        try (Statement st = conn.createStatement()) {
            for (String sql : sqls) st.execute(sql);
        }
    }

    private static void queryAndPrintResult(Connection conn) throws SQLException {
        String sql =
            "SELECT " +
            " p.AMOUNT AS SALARY, " +
            " e.FIRST_NAME || ' ' || e.LAST_NAME AS NAME, " +
            " (CAST(strftime('%Y', p.PAYMENT_TIME) AS INTEGER) - CAST(strftime('%Y', e.DOB) AS INTEGER) " +
            "  - (strftime('%m-%d', p.PAYMENT_TIME) < strftime('%m-%d', e.DOB))) AS AGE, " +
            " d.DEPARTMENT_NAME " +
            "FROM PAYMENTS p " +
            "JOIN EMPLOYEE e ON e.EMP_ID = p.EMP_ID " +
            "JOIN DEPARTMENT d ON d.DEPARTMENT_ID = e.DEPARTMENT " +
            "WHERE CAST(strftime('%d', p.PAYMENT_TIME) AS INTEGER) <> 1 " +
            "ORDER BY p.AMOUNT DESC " +
            "LIMIT 1;";

        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(sql)) {
            if (rs.next()) {
                System.out.println("SALARY = " + rs.getDouble("SALARY"));
                System.out.println("NAME   = " + rs.getString("NAME"));
                System.out.println("AGE    = " + rs.getInt("AGE"));
                System.out.println("DEPT   = " + rs.getString("DEPARTMENT_NAME"));
            }
        }
    }
}
