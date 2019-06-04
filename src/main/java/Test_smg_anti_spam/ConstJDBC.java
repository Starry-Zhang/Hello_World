package Test_smg_anti_spam;


import java.sql.*;

public class ConstJDBC {
    static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    static final String DB_URL = "jdbc:mysql://localhost:3306/testupdatebroadcast";
    static final String USER = "root";
    static final String PASS = "adminroot";
    public Connection getConnection() throws ClassNotFoundException, SQLException {
        Connection conn = null;
        // 注册 JDBC 驱动
        Class.forName("com.mysql.jdbc.Driver");
        // 打开链接
        return  conn = DriverManager.getConnection(DB_URL, USER, PASS);
    }
    public Statement getStatement() throws SQLException, ClassNotFoundException {
        Connection conn=getConnection();
        Statement stmt = null;
        stmt = conn.createStatement();
        return stmt;
    }

    public void close() throws SQLException, ClassNotFoundException {
        getConnection().close();
        getStatement().close();
    }

    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        ConstJDBC con=new ConstJDBC();
        Statement statement=con.getStatement();
        ResultSet rs = statement.executeQuery("select * from testUpdate");
        while (rs.next()) {
            String id=rs.getString("id");
            String name=rs.getString("name");
            System.out.println(id+","+name);
        }
        rs.close();
        con.close();
    }
}
