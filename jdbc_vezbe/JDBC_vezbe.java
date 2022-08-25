/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package jdbc_vezbe;


import java.sql.*;
import java.util.logging.Level;
import java.util.logging.Logger;
/**
 *
 * @author Tasha
 */
public class JDBC_vezbe {

    public static void dodajAdresuBezAutomatskiGenerisanogID(int SifA, String Grad, String Ulica, int Broj){
        Connection conn=DB.getInstance().getConnection();
        String query="select * from Adresa where SifA= ?";
        try (PreparedStatement stmt=conn.prepareStatement(query)){
            stmt.setInt(1, SifA);
            try (ResultSet rs=stmt.executeQuery()){
                if(rs.next()){
                    System.out.println("Adresa sa zadatok SifA vec postoji");
                    return;
                }
            } catch (SQLException ex) {
                Logger.getLogger(JDBC_vezbe.class.getName()).log(Level.SEVERE, null, ex);
            }
        } catch (SQLException ex) {
            Logger.getLogger(JDBC_vezbe.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        query="insert into Adresa (Grad, Ulica, Broj, SifA) values(?, ?, ?, ?)";
        try (PreparedStatement ps=conn.prepareStatement(query)){
            ps.setString(1, Grad);
            ps.setString(2, Ulica);
            ps.setInt(3, Broj);
            ps.setInt(4, SifA);
            ps.executeUpdate();
            System.out.println("Kreirana je nova Adresa kojoj sa zadatom SifA "); 
        } catch (SQLException ex) {
            Logger.getLogger(JDBC_vezbe.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
     
    public static void dodajAdresuSaAutomatskiGenerisanimID(String Grad, String Ulica, int Broj){
        Connection conn=DB.getInstance().getConnection();
        String query="insert into Adresa_Identity (Grad, Ulica, Broj) values(?, ?, ?)";
        try (PreparedStatement ps=conn.prepareStatement(query, PreparedStatement.RETURN_GENERATED_KEYS)){
            ps.setString(1, Grad);
            ps.setString(2, Ulica);
            ps.setInt(3, Broj);
            ps.executeUpdate();
            ResultSet rs=ps.getGeneratedKeys();
            if(rs.next())
                System.out.println("Kreirana je nova Adresa kojoj je automatski dodeljena SifA "+rs.getInt(1)); 
        } catch (SQLException ex) {
            Logger.getLogger(JDBC_vezbe.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
      
    public static void ispisRadnika(){
        Connection conn=DB.getInstance().getConnection();
        try (
            Statement stmt=conn.createStatement();
            ResultSet rs=stmt.executeQuery("select * from Radnik")){
            ResultSetMetaData rsmd=rs.getMetaData();
            for(int i=1; i<=rsmd.getColumnCount(); i++){
                System.out.print(String.format("%-18s", rsmd.getColumnName(i)));
            }        
            System.out.println();
            while(rs.next()){
                for(int i=1; i<=rsmd.getColumnCount(); i++){    
                    if(rsmd.getColumnType(i)==java.sql.Types.INTEGER)
                        System.out.print(String.format("%-18d", rs.getInt(i)));
                    else if(rsmd.getColumnType(i)==java.sql.Types.VARCHAR ||
                            rsmd.getColumnType(i)==java.sql.Types.CHAR)
                        System.out.print(String.format("%-18s", rs.getString(i)));
                }
                System.out.println();
            }
        } catch (SQLException ex) {
            Logger.getLogger(JDBC_vezbe.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    public static void ispisVlasnika(){
        Connection conn=DB.getInstance().getConnection();
        String query="select * from Radnik join Vlasnik on Radnik.BrLK=Vlasnik.BrLK"
                + " join Adresa on Radnik.SifA=Adresa.SifA";
        try (
            PreparedStatement stmt=conn.prepareStatement(query);
            ResultSet rs=stmt.executeQuery()){
            System.out.println("Vlasnici");
            while(rs.next()){
                System.out.println(rs.getString("Ime")+" "
                        +rs.getString("Prezime")+" "+rs.getString("Ulica"));               
            }
        } catch (SQLException ex) {
            Logger.getLogger(JDBC_vezbe.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    public static void izmenaAdreseVlasnika(int SifA){
        Connection conn=DB.getInstance().getConnection();
        String query="select * from Radnik join Vlasnik on Radnik.BrLK=Vlasnik.BrLK";
        try (
            PreparedStatement stmt=conn.prepareStatement(query, 
                    ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
            ResultSet rs=stmt.executeQuery()){
            
            while(rs.next()){
                rs.updateInt("SifA", SifA);
                rs.updateRow();
            }
            System.out.println("Izmenjena je adresa vlasnika");
        } catch (SQLException ex) {
            Logger.getLogger(JDBC_vezbe.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static int brRadnikaSaImenom(String ime)
    {
        Connection conn=DB.getInstance().getConnection();
        String query="{ call SPBrojRadnikaSaImenom (?,?) }"; 
        try (CallableStatement cs= conn.prepareCall(query)){
            cs.setString(1, ime);
            cs.registerOutParameter(2, java.sql.Types.INTEGER);
            cs.execute();
            return cs.getInt(2);
        } catch (SQLException ex) {
            Logger.getLogger(JDBC_vezbe.class.getName()).log(Level.SEVERE, null, ex);
        }
        return 0;
    }
    
    public static void radniciSaImenom(String ime)
    {
        Connection conn=DB.getInstance().getConnection();
        String query="{ call SPRadniciSaImenom (?) }";
        try (CallableStatement cs= conn.prepareCall(query)){     
            cs.setString(1, ime);
            try (ResultSet rs=cs.executeQuery()){
                System.out.println("Radnici sa imenom "+ime);
                while(rs.next())
                    System.out.println("BrLK:"+rs.getInt("BrLK")+
                            " prezime:"+rs.getString("Prezime"));
            } catch (SQLException ex) {
                Logger.getLogger(JDBC_vezbe.class.getName()).log(Level.SEVERE, null, ex);
            }
        } catch (SQLException ex) {
            Logger.getLogger(JDBC_vezbe.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    public static void spisakTabela(){
        Connection conn=DB.getInstance().getConnection();
        try {
            DatabaseMetaData dbmd = conn.getMetaData();
            try( ResultSet rs = dbmd.getTables( null,"dbo", null, null);){
                while(rs.next()) {  
                    System.out.println(rs.getString("TABLE_NAME"));   
                    try( ResultSet rs2 = dbmd.getColumns(null, null,rs.getString("TABLE_NAME"), null);){
                        while(rs2.next()) {
                            System.out.println("   - "+rs2.getString("COLUMN_NAME"));   
                        }
                    }catch (SQLException ex) {
                        Logger.getLogger(JDBC_vezbe.class.getName()).log(Level.SEVERE, null, ex);
                    }
                 } 
            }catch (SQLException ex) {
                Logger.getLogger(JDBC_vezbe.class.getName()).log(Level.SEVERE, null, ex);
            }
        } catch (SQLException ex) {
            Logger.getLogger(JDBC_vezbe.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    public static void spisakProceduraIFunkcija(){
        Connection conn=DB.getInstance().getConnection();
        try {
            DatabaseMetaData dbmd = conn.getMetaData();
            try( ResultSet rs = dbmd.getProcedures(null, "dbo", null)){
                while(rs.next()) {  
                    System.out.println(rs.getString("PROCEDURE_NAME"));   
                 } 
            }catch (SQLException ex) {
                Logger.getLogger(JDBC_vezbe.class.getName()).log(Level.SEVERE, null, ex);
            }
        } catch (SQLException ex) {
            Logger.getLogger(JDBC_vezbe.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    public static void main(String[] args) {
        
        System.out.println("------------------------------------------------------------");
        System.out.println("------------------------------------------------------------");
        System.out.println("Primer dodavanja podataka koriscenjem PreparedStatement");
        System.out.println("------------------------------------------------------------");   
        dodajAdresuBezAutomatskiGenerisanogID(10, "Beograd", "Vojvode Stepe", 39);
        dodajAdresuSaAutomatskiGenerisanimID("Beograd", "Vojvode Stepe", 39);

        System.out.println("------------------------------------------------------------");
        System.out.println("------------------------------------------------------------");
        System.out.println("Primer ispisa tabele i koriscenja MetaData podataka");
        System.out.println("------------------------------------------------------------");   
        ispisRadnika();
        
        System.out.println("------------------------------------------------------------");
        System.out.println("------------------------------------------------------------");
        System.out.println("Primer poziva procedura");
        System.out.println("------------------------------------------------------------");   
        ispisVlasnika();
        System.out.println("----------------------------");
        izmenaAdreseVlasnika(2);
        System.out.println("----------------------------");
        ispisVlasnika();
        
        System.out.println("------------------------------------------------------------");
        System.out.println("------------------------------------------------------------");
        System.out.println("Primer poziva procedura");
        System.out.println("------------------------------------------------------------");   
        radniciSaImenom("Jovan");
        System.out.println("----------------------------");
        System.out.println("broj radnika sa imenom Jovan:"+brRadnikaSaImenom("Jovan"));
        
        System.out.println("------------------------------------------------------------");
        System.out.println("------------------------------------------------------------");
        System.out.println("Spisak tabela koje postoje u bazi");
        spisakTabela();
        System.out.println("----------------------------");
        System.out.println("Spisak procedura i funkcija koje postoje u bazi");
        spisakProceduraIFunkcija();
        
        System.out.println("------------------------------------------------------------");
        System.out.println("------------------------------------------------------------");
    }

}
