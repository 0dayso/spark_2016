package p1;

import java.io.FileInputStream;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Scanner;

public class test {
	public static void main(String[] args) throws Exception {
		ServerSocket server;
		Socket socket;
		String str = null;
		PrintWriter writer;
		server = new ServerSocket(9001);

		socket = server.accept();
		InputStream in = new FileInputStream("dns3.txt");
		

		writer = new PrintWriter(socket.getOutputStream(), true);
        while(true){
        	Scanner sc = new Scanner(in);
        	while (sc.hasNextLine()) {
    			str = sc.nextLine();
    			if (str.length() > 3) {
    				writer.println(str);
    				System.out.println(str);
    			}
    		}
    		writer.close();
    		Thread.sleep(3);
        }
		

	}

}
