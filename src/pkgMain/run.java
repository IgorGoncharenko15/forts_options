package pkgMain;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;
import pkgScroll.Person;
import pkgScroll.VolSmile;

public class run {

    public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException, FileNotFoundException, ParseException {

        
        Connection con = getMysql();
        String root = "/home/igor/Data/forts_vol/";
        String symbol = "LKOH-";
        String date_from_str = "20180601";
        
        VolSmile y = new VolSmile();
        y.getVol2Sql_symbol(con, root, symbol, date_from_str);
        
        System.out.println("Done!");
    }
    
    
    
    
    public static Connection getMysql() throws ClassNotFoundException, SQLException{
          Class.forName("com.mysql.jdbc.Driver");

        String url = "jdbc:mysql://127.0.0.1/igor";
         Properties p = new Properties();
            //p.setProperty("useUnicode","true");
            //p.setProperty("characterEncoding","UTF-8");           
         
           // p.put("charSet", "windows-1251");
         //   p.setProperty("useUnicode","true");
          //  p.put("characterEncoding","cp1251");   
            p.put("user","root");
            p.put("password","1");
        Connection c = DriverManager.getConnection(url,p);
        return c;
    }

}
