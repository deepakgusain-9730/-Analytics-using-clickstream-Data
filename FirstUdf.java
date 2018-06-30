import java.util.Calendar;
import java.text.DateFormat;
import org.apache.hadoop.hive.ql.exec.UDF; 
import org.apache.hadoop.io.Text;
public class FirstUdf extends UDF{
	Calendar cal = Calendar.getInstance();
	public Text evaluate(Text text){
		if(text==null) return null;
		 int year = cal.get(Calendar.YEAR);
		 int month = cal.get(Calendar.MONTH); 
		 int day = cal.get(Calendar.DAY_OF_MONTH);
		 String sg=text.toString();
		 if(sg!=null)
		 {
	String s[]=sg.split("-");
	  int k=0;
	  int dy=Integer.parseInt(s[0]);
	  int yr=Integer.parseInt(s[2]);
	  String mnth[]={"jan","feb","mar","apr","may","jun","jul","aug","sep","oct","nov","dec"};
	  for(int i=0;i<mnth.length;i++)
	  {
		  String st=s[1].toLowerCase();
		  if(st.equals(mnth[i]))
			  break;
		  k++;
	  }
	  if(month<k)
		  year--;
	  int r=year-yr;
	  
	  return new Text(String.valueOf(r%100));
		
	}
		 return new Text(sg);
	
}
}