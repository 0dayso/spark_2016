package p1;

import java.io.FileInputStream;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Scanner;


public class ProcessDNS {
	
	public static void main(String[] args) throws Exception{
		ServerSocket server;
		Socket socket;
		String str =null;
		PrintWriter writer;
		while(true){
			Process proc = Runtime.getRuntime().exec("analysis.py");
			proc.waitFor();
			InputStream  in =new FileInputStream("new.dns");
			Scanner sc = new Scanner(in);
			server = new ServerSocket(9001);
			socket = server.accept();
			writer = new PrintWriter(socket.getOutputStream(),true);
			
			while(sc.hasNextLine()){
				str = sc.nextLine();
				if(str.length()>3){
					writer.println(str);
					System.out.println(str);
				}
			}
			writer.close();			
		}
		
	}
      
}
