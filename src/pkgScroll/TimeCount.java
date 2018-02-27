
package pkgScroll;

public class TimeCount {
    public int date_long;
    public long datetime_long;
    public int line_count;
    public String date_str; 
    public String instrument;
    public String line;
    public String underlying ;
    
    
    public double future_price;
    public double s;
    public double a;
    public double b;
    public double c;
    public double d;
    public double e;
    public double t;

    public long maturity_date;
    
    public TimeCount(int date_long, long datetime_long, int line_count, String date_str, String instrument, String line) {
        this.date_long = date_long;
        this.datetime_long = datetime_long;
        this.line_count = line_count;
        this.date_str = date_str;
        this.instrument = instrument;
        this.line = line;
    }

    public int getDate(){
        return this.date_long;
    }
    public long getDateTime(){
        return this.datetime_long;
    }
    public String getInstrument(){
        return this.instrument;
    }
    public String getLine(){
        return this.line;
    }
    public double getTime2Maturity(){
        return this.t;
    }

    public void setMaturity(){
        this.maturity_date = this.datetime_long + (long) (this.t*24*365*3600*1000);
    }
    public void setUnderlying(){
         this.underlying = this.instrument.split("-")[0];
    }

    
}
