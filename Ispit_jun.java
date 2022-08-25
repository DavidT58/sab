package ispit_jun;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 *
 * @author Tasha
 */
public class Ispit_jun {

    private Connection connection=DB.getInstance().getConnection();

    private void AzuriranjeBaze(int SifD, int sektor, int redBroj, int sediste, int brojUlaznica){
        try(PreparedStatement stUpdate=connection.prepareStatement(
                "UPDATE ULAZNICA SET Status='P' " +
                        "WHERE SifU IN (SELECT SifU FROM VAZI WHERE SifD=?)  AND SedisteBr>=? AND  SedisteBr<? " +
                        "AND SifR IN (SELECT SifR FROM Red WHERE SifS=? AND Broj=?)");
                PreparedStatement stUpdateDogadjaj=connection.prepareStatement(
                        "UPDATE Dogadjaj set BrojPreostalihUlaznica = BrojPreostalihUlaznica - ? WHERE SifD=?")
        ){
            stUpdate.setInt(1,SifD);
            stUpdate.setInt(2,sediste);
            stUpdate.setInt(3,sediste+brojUlaznica);
            stUpdate.setInt(4,sektor);
            stUpdate.setInt(5,redBroj);
            stUpdate.executeUpdate();

            stUpdateDogadjaj.setInt(1,brojUlaznica);
            stUpdateDogadjaj.setInt(2,SifD);
            stUpdateDogadjaj.executeUpdate();
        }catch (SQLException s) {
            s.printStackTrace();
        }
    }
    private boolean KupovinaKarataUJednomRedu(int SifD, int BrojUlaznica){
        try (PreparedStatement st = connection.prepareStatement(
                "SELECT TOP 1  S.SifS,  U.SifR,  R.Broj, U.SedisteBr " +
                        "FROM VAZI V JOIN ULAZNICA U ON V.SifU=U.SifU JOIN RED R ON U.SifR=R.SifR JOIN SEKTOR S ON R.SifS=S.SifS " +
                        "WHERE V.SifD=? AND ( " +
                        "SELECT COUNT(*) FROM VAZI V2 JOIN ULAZNICA U2 ON V2.SifU=U2.SifU WHERE U2.SifR=U.SifR "+
                        "AND V2.SifD=? AND U2.SedisteBr<U.SedisteBr+? AND U2.SedisteBr>=U.SedisteBr  AND U2.Status='S' " +
                        ")=? ORDER BY S.FaktorS DESC, R.FaktorR DESC, U.SedisteBr DESC")) {
            st.setInt(1,SifD);
            st.setInt(2,SifD);
            st.setInt(3,BrojUlaznica);
            st.setInt(4,BrojUlaznica);
            try(ResultSet rs=st.executeQuery()){
                if(rs.next()) {
                    AzuriranjeBaze(SifD, rs.getInt(1), rs.getInt(3), rs.getInt(4), BrojUlaznica);
                    return  true;
                }
            }catch (SQLException s) {
                s.printStackTrace();
            }
        } catch (SQLException s) {
            s.printStackTrace();
        }
        return false;
    }

    public boolean SlobodneUlaznice(int SifD, int sektor, int brojReda, int sediste, int brojUlaznica){
        try (PreparedStatement st = connection.prepareStatement(
                "SELECT COUNT(*) " +
                        "FROM VAZI V JOIN ULAZNICA U ON V.SifU=U.SifU JOIN RED R ON U.SifR=R.SifR " +
                        "WHERE V.SifD=? AND R.SifS=? AND R.Broj=?  AND U.SedisteBr>=? AND U.SedisteBr<?  AND U.Status='S'")) {
            st.setInt(1,SifD);
            st.setInt(2,sektor);
            st.setInt(3,brojReda);
            st.setInt(4,sediste);
            st.setInt(5,sediste+brojUlaznica);
            try(ResultSet rs=st.executeQuery()){
                if(rs.next()) {
                    return  rs.getInt(1)==brojUlaznica;
                }
            }catch (SQLException s) {
                s.printStackTrace();
            }
        } catch (SQLException s) {
            s.printStackTrace();
        }
        return false;
    }

    private boolean KupovinaKarataUDvaReda(int SifD, int BrojUlaznica){
        try (PreparedStatement st = connection.prepareStatement(
                "SELECT S.SifS,  U.SifR,  R.Broj, U.SedisteBr " +
                        "FROM VAZI V JOIN ULAZNICA U ON V.SifU=U.SifU JOIN RED R ON U.SifR=R.SifR JOIN SEKTOR S ON R.SifS=S.SifS " +
                        "WHERE V.SifD=? AND ( " +
                        "SELECT COUNT(*) FROM VAZI V2 JOIN ULAZNICA U2 ON V2.SifU=U2.SifU WHERE U2.SifR=U.SifR "+
                        "AND V2.SifD=? AND U2.SedisteBr<U.SedisteBr+? AND U2.SedisteBr>=U.SedisteBr  AND U2.Status='S' " +
                        ")=? ORDER BY S.FaktorS DESC, R.FaktorR DESC, U.SedisteBr DESC")) {
            st.setInt(1,SifD);
            st.setInt(2,SifD);
            st.setInt(3,BrojUlaznica/2);
            st.setInt(4,BrojUlaznica/2);
            try(ResultSet rs=st.executeQuery()){
                while(rs.next()) {
                    int sektor=rs.getInt(1);
                    int redBroj=rs.getInt(3);
                    int sediste=rs.getInt(3);
                    if(BrojUlaznica%2==0){
                        if(SlobodneUlaznice(SifD,sektor, redBroj+1,sediste,BrojUlaznica/2)) {
                            AzuriranjeBaze(SifD, sektor, redBroj, sediste, BrojUlaznica / 2);
                            AzuriranjeBaze(SifD, sektor, redBroj+1, sediste, BrojUlaznica / 2);
                            return true;
                        }
                    }
                    else {
			int[][] test_red_sediste=new int[][]{
                            {redBroj+1,sediste},{redBroj+1,sediste-1},
                            {redBroj-1,sediste},{redBroj-1,sediste-1}};
                        for(int i=0; i<test_red_sediste.length; i++){
                            if(SlobodneUlaznice(SifD,sektor, test_red_sediste[i][0],test_red_sediste[i][1],(BrojUlaznica+1)/2)) {
                                AzuriranjeBaze(SifD, sektor, redBroj, sediste, BrojUlaznica / 2);
                                AzuriranjeBaze(SifD, sektor, test_red_sediste[i][0],test_red_sediste[i][1], (BrojUlaznica+1) / 2);
                                return true;
                            }
                        }
                    }
                }
            }catch (SQLException s) {
                s.printStackTrace();
            }
        } catch (SQLException s) {
            s.printStackTrace();
        }
        return false;
    }

    public void SlobodnaProdajaUlaznica(String NazivDogadjaja, int BrojUlaznica){
        int SifD=-1;
        try (PreparedStatement st = connection.prepareStatement(
                "SELECT TOP 1 SifD FROM DOGADJAJ WHERE Naziv=? AND Datum=CONVERT(DATE, GETDATE())")) {
            st.setString(1,NazivDogadjaja);
            try(ResultSet rs=st.executeQuery()){
                if(rs.next())
                    SifD=rs.getInt(1);
                else {
                    System.out.println("Karte za dogadjaj "+NazivDogadjaja+" nisu u prodaji");
                    return;
                }
            }catch (SQLException s) {
                s.printStackTrace();
            }
        } catch (SQLException s) {
            s.printStackTrace();
        }

        if (KupovinaKarataUJednomRedu(SifD,BrojUlaznica)){
            System.out.println("Karte za dogadjaj "+NazivDogadjaja+" su kupljene u jednom redu");
            return;
        }
        if (KupovinaKarataUDvaReda(SifD,BrojUlaznica)){
            System.out.println("Karte za dogadjaj "+NazivDogadjaja+" su kupljene u dva reda");
            return;
        }
        System.out.println("Karte za dogadjaj "+NazivDogadjaja+" nisu kupljene");
    }

    /***
     *
     * main metoda nece biti testirana, u njoj mozete pisati vase testove
     */
    public static void main(String[] args) {
       (new Ispit_jun()).SlobodnaProdajaUlaznica("D1",3);
    }

}
