package application;

import db.DB;
import db.DbException;

import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class Program {

    public static void main(String[] args) {

        Connection conn = DB.getConnection();

        // Recuperar dados
        /*
        Statement st = null;
        ResultSet rs = null;
        try {
            conn = DB.getConnection();
            st = conn.createStatement();
            rs = st.executeQuery("select * from department");
            while (rs.next()){
                System.out.println(rs.getInt("id") + " - " + rs.getString("name"));
            }
        } catch (SQLException e) {
            throw  new DbException(e.getMessage());
        } finally {
            DB.closeResultSet(rs);
            DB.closeStatement(st);
            DB.getConnection();
        }
        */

        // Inserir dados
        PreparedStatement st = null;
        SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");
        try {
            conn = DB.getConnection();

            // Inserção de dados com um único valor
            /*
            st = conn.prepareStatement(
                    "insert into seller " +
                            "(Name, Email, BirthDate, BaseSalary, DepartmentId) " +
                            "values (?, ?, ?, ?, ?)",
                    Statement.RETURN_GENERATED_KEYS);
            st.setString(1, "Carl Purple");
            st.setString(2, "carl@gmail.com");
            st.setDate(3, new java.sql.Date(sdf.parse("22/04/1985").getTime()));
            st.setDouble(4, 3000.0);
            st.setInt(5, 4);
            */

            // Inserção de dados com dois valores ao mesmo tempo
            st = conn.prepareStatement("insert into department (Name) values ('D1'),('D2')",
                    Statement.RETURN_GENERATED_KEYS);

            int rowsAffected = st.executeUpdate();

            if(rowsAffected > 0) {
                ResultSet rs = st.getGeneratedKeys();
                while(rs.next()){
                    int id = rs.getInt(1);
                    System.out.println("Done! ID = " + id);
                }
            }else{
                System.out.println("No rows affected");
            }
        } catch (SQLException e) {
            throw new DbException(e.getMessage());
        } finally {
            DB.closeStatement(st);
            DB.getConnection();
        }
    }
}