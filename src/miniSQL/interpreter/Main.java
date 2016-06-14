package miniSQL.interpreter;

import java.io.File;
import java.util.Scanner;
import java.util.Stack;

public class Main 
{
	public static void main(String[] args)
	{
		System.out.println("Welcome to miniSQL!");
		String cmd = new String();
		String temp = new String();
		Scanner scanner = new Scanner(System.in);
		Stack<Scanner> scannerStack= new Stack<>();
		while (true) {
			if (scannerStack.isEmpty()) {
				System.out.print(">>>");
			}
			if (!scanner.hasNextLine()) {
				scanner.close();
				scanner = scannerStack.pop();
			}
			
			temp = scanner.nextLine();
			temp = temp.trim();
			
			if (temp.indexOf(';')==-1) {
				cmd += " " + temp;
			} else {
				cmd += " " + temp.substring(0,temp.indexOf(";")).trim();
				try
				{
					Parser.parse(cmd.trim());
				} catch (Exception e){
					if (e.getMessage().equals("quit")) {
						break;
					} else if (e.getMessage().startsWith("execfile")) {
						scannerStack.push(scanner);
						String path = e.getMessage().substring(8,e.getMessage().length()).trim();
						try {
							scanner = new Scanner(new File(path));
						} catch (Exception eFile) {
							System.out.println(eFile.getMessage());
							scanner = scannerStack.pop();
						}
					} else {
						System.out.println(e.getMessage());
					}
				}
				cmd = new String();
			}
		}
		scanner.close();
		while (!scannerStack.isEmpty()) {
			scanner = scannerStack.pop();
			scanner.close();
		}
	}

}
