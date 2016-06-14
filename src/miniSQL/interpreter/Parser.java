package miniSQL.interpreter;
import java.io.File;
import java.util.ArrayList;

import miniSQL.api.Table;
import miniSQL.buffer.FileBuffer;

public class Parser
{
	static 
	{
	
	}
	public static void  parse(String cmd) throws Exception
	{
		if (cmd.equals("quit")) {
			System.out.println("Bye");
			throw (new Exception(cmd));
		} else if (cmd.startsWith("execfile")) {
			throw (new Exception(cmd));
		} else if (cmd.startsWith("create table")) {
			createTable(cmd.substring(12,cmd.length()).trim());
		} else if (cmd.startsWith("drop table")) {
			dropTable(cmd.substring(10,cmd.length()).trim());
		} else if (cmd.startsWith("create index")) {
			createIndex(cmd.substring(12,cmd.length()).trim());
		} else if (cmd.startsWith("drop index")) {
			System.out.println("drop index");
		} else if (cmd.startsWith("select")) {
			System.out.println("select");
		} else if (cmd.startsWith("insert into")) {
			System.out.println("insert into");
		} else if (cmd.startsWith("delete from")) {
			System.out.println("delete from");
		} else {
			throw (new Exception("Invalid Syntax."));
		}
	}
	private static void createTable(String cmd) throws Exception
	{
		if (cmd.indexOf("(") != -1) {
			String tableName = cmd.substring(0,cmd.indexOf("(")).trim();
		} else {
			throw (new Exception("Invalid Syntax."));
		}
		cmd = cmd.substring(cmd.indexOf("("),cmd.length());
		if (cmd.endsWith(")")) {
			cmd = cmd.substring(1,cmd.length()-1);
			String[] columnDef = cmd.split(",");
			for (String iString : columnDef) {
				System.out.println(iString);
			}
		} else {
			throw (new Exception("Invalid Syntax."));
		}
	}
	private static void dropTable(String cmd) throws Exception
	{
		File file = new File(cmd);
		if (file.exists()) {
			file.delete();
			System.out.println("Drop table finished.");
		} else {
			throw (new Exception("No such table."));
		}
	}
	private static void createIndex(String cmd) throws Exception
	{
		FileBuffer fileBuffer = new FileBuffer();
		//TODO
		Table indexTable = new Table(fileBuffer);
		
	}
	
}
