package application;

import db.DB;
import db.DbException;
import db.DbIntegrityException;

import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class Program {

    public static void main(String[] args) {
        Connection conn = DB.getConnection();

        // Recuperar dados
        // recuperarDados(conn);

        // Inserir único dados
        // inserirUnicoDados(conn);

        // Inserir vários dados
        // inserirVariosDados(conn);

        // Atualizar dados
        // atualizarDados(conn);

        // Deletar dados
        // deletarDados(conn);

        // Transações
        // transacoes(conn);

    }

    private static void recuperarDados(Connection conn){
        Statement st = null;
        ResultSet rs = null;
        try {
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
    }

    private static void inserirUnicoDados(Connection conn){
        PreparedStatement st = null;
        SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");
        try {
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
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    private static void inserirVariosDados(Connection conn) {
        PreparedStatement st = null;
        SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");
        try {
            st = conn.prepareStatement("insert into department (Name) values ('D1'),('D2')",
                    Statement.RETURN_GENERATED_KEYS);

            int rowsAffected = st.executeUpdate();

            if (rowsAffected > 0) {
                ResultSet rs = st.getGeneratedKeys();
                while (rs.next()) {
                    int id = rs.getInt(1);
                    System.out.println("Done! ID = " + id);
                }
            } else {
                System.out.println("No rows affected");
            }
        } catch (SQLException e) {
            throw new DbException(e.getMessage());
        } finally {
            DB.closeStatement(st);
            DB.getConnection();
        }
    }

    private static void atualizarDados(Connection conn){
        PreparedStatement st = null;
        try {
            st = conn.prepareStatement("update seller set BaseSalary = BaseSalary + ? where (DepartmentId = ?)");
            st.setDouble(1, 200.0);
            st.setInt(2, 2);

            int rowsAffected = st.executeUpdate();

            System.out.println("Done! Rows affected: " + rowsAffected);
        } catch (SQLException e){
            e.printStackTrace();
        } finally {
            DB.closeStatement(st);
            DB.closeConnection();
        }
    }

    private static void deletarDados(Connection conn){
        PreparedStatement st = null;
        try {
            st = conn.prepareStatement("delete from department where Id = ?");
            st.setInt(1, 2);
            int rowsAffected = st.executeUpdate();
            System.out.println("Done! Rows affected: " + rowsAffected);
        } catch (SQLException e){
            throw new DbIntegrityException(e.getMessage());
        } finally {
            DB.closeStatement(st);
            DB.closeConnection();
        }
    }

    private static void transacoes(Connection conn){
        Statement st = null;
        try {
            st = conn.createStatement();

            conn.setAutoCommit(false);

            int rows1 = st.executeUpdate("update seller set BaseSalary = 2090 where DepartmentId = 1");

            // int x = 1;
            // if(x < 2){
            //     throw new SQLException("Fake error");
            // }

            int rows2 = st.executeUpdate("update seller set BaseSalary = 3090 where DepartmentId = 2");

            conn.commit();

            System.out.println("Rows1: " + rows1);
            System.out.println("Rows2: " + rows2);
        } catch (SQLException e){
            try {
                conn.rollback();
                throw  new DbException("Transaction rolled back! Caused by: " + e.getMessage());
            } catch (SQLException e1) {
                throw  new DbException("Error trying to rollback! Caused by: " + e.getMessage());
            }
        } finally {
            DB.closeStatement(st);
            DB.closeConnection();
        }
    }
}