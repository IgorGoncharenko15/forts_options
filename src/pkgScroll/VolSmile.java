
package pkgScroll;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Scanner;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class VolSmile {
    
    public void getVol2Sql_symbol(Connection con, String root, String symbol, String date_from_str) throws SQLException, FileNotFoundException, IOException, ParseException{
        Statement st = con.createStatement();
        
        SimpleDateFormat dateformat_long = new SimpleDateFormat("yyyyMMddHHmmssSSS");
        SimpleDateFormat dateformat_short = new SimpleDateFormat("yyyyMMdd");
        
        Date date_from = dateformat_short.parse(date_from_str);
        
        File folder = new File(root);
        File[] files_list = folder.listFiles();
        /*
        ResultSet res = st.executeQuery("select distinct instrument_name from igor.moex_instrument");
        
        HashSet instrument_inDB = new HashSet<String>(); 
        
        while (res.next()){
            instrument_inDB.add(res.getString(1));
        }
        res.close();
        */
         
        
        for (int i = 0; i < files_list.length; i++) {
            
        
            if (files_list[i].getName().contains(".csv")){
                
                Date fileDate =  dateformat_short.parse(files_list[i].getName().split(".c")[0]+"01");
                
                
                if (fileDate.getTime()<=date_from.getTime()){
        
                    List<TimeCount> line2write = new ArrayList<TimeCount>();
                    HashSet instrument_name = new HashSet<String>();
                    
                    BufferedReader br = new BufferedReader(new FileReader(root + files_list[i].getName()));

                    String line = br.readLine();
                    line = br.readLine();
                    int counter = 1;
                    
                    List<TimeCount> lines_counter = new ArrayList<TimeCount>();
                    
                    while (line != null){
                        if(line.length()>symbol.length()){
                        if(line.substring(0, symbol.length()).matches(symbol)){
                            
                            Scanner sc = new Scanner(line);
                            sc.useDelimiter(";");
                            String instrument_run = sc.next();
                            instrument_name.add(instrument_run);
                            sc.next();
                            String datetime_run = sc.next();
                            long datetime_run_long = dateformat_long.parse(datetime_run).getTime();
                            long datetime_from = dateformat_long.parse(datetime_run + "100000000").getTime();
                            long datetime_to = dateformat_long.parse(datetime_run + "190000000").getTime();
                            
                            if ( (datetime_run_long>=datetime_from) && (datetime_run_long<=datetime_to) ){
                                lines_counter.add(new TimeCount(
                                        (int) Math.floor(dateformat_short.parse(datetime_run.substring(0, 8)).getTime()/1000/3600/24),
                                        datetime_run_long,
                                        counter,
                                        datetime_run,
                                        instrument_run,
                                        line
                                        ));
                            }
                            
                        }}
                        
                        line = br.readLine();
                        counter++;
                    }
                    
                    Iterator<String> itr = instrument_name.iterator();
                    while(itr.hasNext()){
                       
                    String instrument_run = itr.next();
                    Stream<TimeCount> instrumentStream = lines_counter
                    .stream()
                    .filter(instr -> instr.getInstrument().matches(instrument_run));
                    
                        
                        
                    Map<Integer, List<TimeCount>> timeCountbyDate = instrumentStream
                    .collect(Collectors.groupingBy(TimeCount::getDate));
        
                        for (int j : timeCountbyDate.keySet()) {
                            
                            
                            Optional<TimeCount> last_time = timeCountbyDate.get(j)
                                    .stream()
                            .collect(Collectors.maxBy(Comparator.comparing(TimeCount::getDateTime)));
                            TimeCount data2write = last_time.get();
                            Scanner sc_line = new Scanner(data2write.line);
                            sc_line.useDelimiter(";");
                            data2write.date_long = (int) Math.floor(dateformat_short.parse(data2write.date_str.substring(0, 8)).getTime()/1000/3600/24);
                            
                                  sc_line.next();sc_line.next();sc_line.next();
                                  data2write.future_price = Double.parseDouble(sc_line.next().replaceAll(",", ".").toLowerCase());
                                  data2write.s = Double.parseDouble(sc_line.next().replaceAll(",", ".").toLowerCase());
                                  data2write.a = Double.parseDouble(sc_line.next().replaceAll(",", ".").toLowerCase());
                                  data2write.b = Double.parseDouble(sc_line.next().replaceAll(",", ".").toLowerCase());
                                  data2write.c = Double.parseDouble(sc_line.next().replaceAll(",", ".").toLowerCase());
                                  data2write.d = Double.parseDouble(sc_line.next().replaceAll(",", ".").toLowerCase());
                                  data2write.e = Double.parseDouble(sc_line.next().replaceAll(",", ".").toLowerCase());
                                  data2write.t = Double.parseDouble(sc_line.next().replaceAll(",", ".").toLowerCase());
                                  data2write.setMaturity();
                                  data2write.setUnderlying();
                                  if (data2write.t>0.0){
                                    line2write.add(data2write);
                                  }
                        }
                    
                    
                    }
                    System.out.println(fileDate + " is collected ");
                    
                    Map<Integer, List<TimeCount>> line2write_map = line2write.stream()
                    .collect(Collectors.groupingBy(TimeCount::getDate));

                    String query = "insert into igor.moex_volatility values (?,?,?,?,?, ?,?,?,?,?, ?,?,?,?) ";
                    
                    
                    PreparedStatement ps = null;
                    ps = con.prepareStatement(query);
                    
                        for (int k : line2write_map.keySet()) {
                            
                            
                            Stream<TimeCount> ordered_vol = line2write_map.get(k)
                                    .stream()
                                .sorted(Comparator.comparing(TimeCount::getTime2Maturity));

                            Iterator<TimeCount> iterator_write = ordered_vol.iterator();
                            int counter_line = 1;
                            
                            while(iterator_write.hasNext()){
                                TimeCount line_run = iterator_write.next();
                                
                                ps.setDate(1, new java.sql.Date(line_run.datetime_long));
                                ps.setDate(2, new java.sql.Date(line_run.datetime_long));
                                ps.setString(3, line_run.instrument);
                                ps.setString(4, line_run.underlying);
                                ps.setInt(5, counter_line);
                                ps.setDouble(6, line_run.future_price);
                                ps.setDouble(7, line_run.s);
                                ps.setDouble(8, line_run.a);
                                ps.setDouble(9, line_run.b);
                                ps.setDouble(10, line_run.c);
                                ps.setDouble(11, line_run.d);
                                ps.setDouble(12, line_run.e);
                                ps.setDouble(13, line_run.t);
                                
                                ps.setDate(14, new java.sql.Date(line_run.maturity_date));
                                ps.addBatch();
                                counter_line++;
                            }
                        }

                    int[] num_lines = ps.executeBatch();
                    System.out.println("Wrote " + num_lines.length + " lines");

                }
                
                
                
                
            }
        }
}
}
