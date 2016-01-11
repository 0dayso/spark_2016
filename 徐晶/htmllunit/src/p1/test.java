package p1;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.PrintWriter;

import com.gargoylesoftware.htmlunit.NicelyResynchronizingAjaxController;
import com.gargoylesoftware.htmlunit.WebClient;
import com.gargoylesoftware.htmlunit.html.DomElement;
import com.gargoylesoftware.htmlunit.html.HtmlPage;

public class test{
    public static void main(String[] args) throws Exception{
        String str;
        
        
        FileReader fr=new FileReader("domain.txt");
		BufferedReader br =new BufferedReader(fr);
		PrintWriter pt =new PrintWriter("goodbad.txt");
		PrintWriter pt1 =new PrintWriter("left.txt");
		
		String domain=null;
		String type =null;
		Boolean b=true;
		String line =null;
		
		int num=0;
		while ((domain = br.readLine()) != null) {
			num++;
			System.out.println(domain+" "+num);
			
			WebClient webClient = new WebClient();	        
	        webClient.getOptions().setJavaScriptEnabled(true);
	        webClient.getOptions().setCssEnabled(false);
	        webClient.setAjaxController(new NicelyResynchronizingAjaxController());
	        webClient.getOptions().setThrowExceptionOnScriptError(false);
			
			try {
				HtmlPage page = webClient.getPage("http://webscan.360.cn/index/checkwebsite?url="+ domain);
				Thread.sleep(5000);                  
				type = page.getElementById("jg_tips").asText();
				
		        			
			} catch (Exception e) {
				num--;
				pt1.println(domain);
				System.out.println(domain + " " +type); 
				pt1.flush();
				type=".";
			}
			if (!type.contains(".")) {
				pt.println(domain + " " + type);
				System.out.println(domain + " " +type);
				pt.flush();
			}
			webClient.close();
		}

        pt1.close();
        pt.close();
        

    }
}