package miniSQL.interpreter;
import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import miniSQL.api.Column;
import miniSQL.api.Record;
import miniSQL.api.SQLElement;
import miniSQL.api.SQLFloat;
import miniSQL.api.SQLInteger;
import miniSQL.api.SQLPredicate;
import miniSQL.api.SQLString;
import miniSQL.api.Table;
import miniSQL.buffer.FileBuffer;

public class Parser
{
	private static final int LEGNTH = 64;
	static Table indexTable;
	static FileBuffer indexFileBuffer;
	static 
	{
		File file = new File("index");
		indexFileBuffer = new FileBuffer(file);
		indexTable = new Table(indexFileBuffer);
	}
	public static void close()
	{
		indexFileBuffer.close();
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
			dropIndex(cmd.substring(10,cmd.length()).trim());
		} else if (cmd.startsWith("select * from")) {
			selectFrom(cmd.substring(13,cmd.length()).trim());
		} else if (cmd.startsWith("insert into")) {
			insertInto(cmd.substring(11,cmd.length()).trim());
		} else if (cmd.startsWith("delete from")) {
			deleteFrom(cmd.substring(11,cmd.length()).trim());
		} else {
			throw new Exception("Invalid Syntax.");
		}
	}
	private static void createTable(String cmd) throws Exception
	{	String tableName;
		String[] tableDef;
		String[] columnDef;
		String pkName = "";
		if (!(cmd.contains("(")&&cmd.contains(")"))) {
			throw new Exception("Invalid Syntax.");
		} 
		tableName = cmd.substring(0,cmd.indexOf("(")).trim();
		File tableFile = new File(tableName);
		if (tableFile.exists()) {
			throw new Exception("Table already existed.");
		}
		cmd = cmd.substring(cmd.indexOf("(")+1,cmd.length()-1).trim();
		tableDef = cmd.split(",");
		int hasPK = 0;
		for (int i=0;i<tableDef.length;i++) {
			tableDef[i] = tableDef[i].trim();
		}
		for (String s : tableDef) {
			if (s.contains("primary key")) {
				hasPK = 1;
				pkName = s.substring(11,s.length()).trim();
				pkName = pkName.substring(1, pkName.length()-1);
			}
		}
		int pkValid = 0;
		ArrayList<Column> schema = new ArrayList<>();
		SQLElement type;
		int isUnique = 0;
		for (String s : tableDef) {
			if (s.contains("primary key")) {
				continue;
			}
			columnDef = s.split(" ");
			if (columnDef[0].equals(pkName)) {
				pkValid = 1;
			}
			if (columnDef[1].equals("int")) {
				type = new SQLInteger();
			} else if (columnDef[1].equals("float")) {
				type = new SQLFloat();
			} else if (columnDef[1].startsWith("char(")&&columnDef[1].endsWith(")")) {
				int stringLength;
				try {
					stringLength = Integer.parseInt(columnDef[1].substring(5, columnDef[1].length()-1));
				} catch (Exception e) {
					throw new Exception("Invalid type.");
				}
				if (1<=stringLength&&stringLength<=255) {
					type = new SQLString(stringLength);
				} else {
					throw new Exception("Invalid type.");
				}
			} else {
				throw new Exception("Invalid type.");
			}
			if (columnDef.length>=3&&columnDef[2].equals("unique")) {
				isUnique = 1;
			} else if (columnDef.length>=3&&columnDef[2] != null) {
				throw new Exception("Invalid syntax.");
			}
			schema.add(new Column(columnDef[0],type,(pkName.equals(columnDef[0])),(isUnique==1)));
			if (columnDef[0].length()>32) {
				System.out.println("Column Name" + columnDef[0] + "trimed to 32 chars.");
			}
		}
		if (hasPK==1&&pkValid==0) {
			throw new Exception("Primary key constraint failed.");
		}
		if (schema.size()>32) {
			throw new Exception("Too many columns.");
		}
		FileBuffer fileBuffer = new FileBuffer(tableFile);
		Table table = new Table(fileBuffer,schema);
		fileBuffer.close();
		System.out.println("Create table accomplished.");
	}
	private static void dropTable(String cmd) throws Exception
	{
		String tableName = cmd;
		File file = new File(tableName);
		if (!file.exists()) {
			throw new Exception("No such Table.");
		}
		file.delete();
		indexTable.clearSelect();
		indexTable.selectAnd(1, SQLPredicate.EQUAL, new SQLString(tableName,LEGNTH));
		for (Record record : indexTable) {
			record.remove();
		}
		System.out.println("Drop table accomplished.");
	}
	private static void createIndex(String cmd) throws Exception
	{
		String[] indexDef;
		String tableName;
		String indexName;
		String columnName;
		if (!(cmd.contains("on"))) {
			throw new Exception("Invalid Syntax.");
		}
		indexDef = cmd.split("on");
		for (int i=0;i<indexDef.length;i++) {
			indexDef[i] = indexDef[i].trim();
		}
		indexName = indexDef[0];
		if (!(indexDef[1].contains("(")&&indexDef[1].contains(")"))) {
			throw new Exception("Invalid Syntax.");
		}
		tableName = indexDef[1].split("(")[0].trim();
		columnName = indexDef[1].split("(")[1].split(")")[0];
		File tableFile = new File(tableName);
		if (!tableFile.exists()) {
			throw new Exception("No such table.");
		}
		FileBuffer fileBuffer = new FileBuffer(tableFile);
		Table table = new Table(fileBuffer);
		int columnIndex = table.getColumnIndex(columnName);
		if (columnIndex == -1) {
			throw new Exception("No such column.");
		}
		Record record = new Record(indexTable);
		record.set(0, new SQLString(indexName,LEGNTH));
		record.set(1, new SQLString(tableName, LEGNTH));
		record.set(2, new SQLInteger(columnIndex));
		if (!indexTable.insert(record)) {
			fileBuffer.close();
			throw new Exception("Duplicated index name.");
		}
		if (table.getColumns().get(columnIndex).isIndexed()) {
			fileBuffer.close();
			throw new Exception("Index already existed.");
		} else if (!table.getColumns().get(columnIndex).isUnique()) {
			fileBuffer.close();
			throw new Exception("Column is not unique.");
		}
		table.getColumns().get(columnIndex).createIndex();
		fileBuffer.close();
		System.out.println("Create index accomplished.");
	}
	private static void dropIndex(String cmd) throws Exception
	{
		String indexName = cmd;
		String tableName;
		int columnIndex;
		indexTable.clearSelect();
		indexTable.selectAnd(0,SQLPredicate.EQUAL, new SQLString(indexName, LEGNTH));
		Iterator<Record> it = indexTable.iterator();
		if (!it.hasNext()) {
			throw new Exception("No such index.");
		}
		Record record = it.next();
		tableName = record.get(1).toString();
		columnIndex = ((SQLInteger)record.get(2)).toInteger();
		File file = new File(tableName);
		FileBuffer fileBuffer = new FileBuffer(file);
		Table table = new Table(fileBuffer);
		table.getColumns().get(columnIndex).dropIndex();
		record.remove();
		fileBuffer.close();
		System.out.println("Drop index accomplished.");
	}
	private static void insertInto(String cmd) throws Exception
	{
		String tableName;
		String[] values;
		if (!cmd.contains("values")) {
			throw new Exception("Invalid syntax.");
		}
		tableName = cmd.split("values")[0].trim();
		File tableFile = new File(tableName);
		if (!tableFile.exists()) {
			throw new Exception("No such table.");
		}
		cmd = cmd.split("values")[1].trim();
		if (!(cmd.startsWith("(")&&cmd.endsWith(")"))) {
			throw new Exception("Invalid syntax.");
		}
		cmd = cmd.substring(1, cmd.length()-1).trim();
		values = cmd.split(",");
		FileBuffer fileBuffer = new FileBuffer(tableFile);
		Table table = new Table(fileBuffer);
		if (table.getColumns().size()!=values.length) {
			fileBuffer.close();
			throw new Exception("Schema not match.");
		}
		Record record = new Record(table);
		try {
			for (int i=0;i<values.length;i++) {
				record.parse(i,values[i].trim());
			}
		} catch (Exception e) {
			throw new Exception("Schema not match.");
		}
		if (!table.insert(record)) {
			fileBuffer.close();
			throw new Exception("Duplicated value.");
		}
		fileBuffer.close();
		System.out.println("Insert into accomplished.");
	}
	private static void deleteFrom(String cmd) throws Exception
	{
		String tableName;
		String[] conditions;
		String columnName;
		SQLPredicate predicate;
		SQLElement value;
		String[] oneConditon;
		String predicateS;
		tableName = cmd.split(" ")[0];
		File tableFile = new File(tableName);
		if (!tableFile.exists()) {
			throw new Exception("No such table.");
		}
		FileBuffer fileBuffer = new FileBuffer(tableFile);
		Table table = new Table(fileBuffer);
		cmd = cmd.substring(tableName.length(),cmd.length()).trim();
		if (cmd.contains("where")) {
			table.clearSelect();
			cmd = cmd.substring(5,cmd.length());
			conditions = cmd.split("and");
			for (int i = 0;i<conditions.length;i++) {
				conditions[i] = conditions[i].trim();
			}
			for (String s : conditions) {
				if (s.contains("<=")) {
					predicateS = "<=";
				} else if (s.contains(">=")) {
					predicateS = ">=";
				} else if (s.contains("!=")) {
					predicateS = "!=";
				} else if (s.contains("<")) {
					predicateS = "<";
				} else if (s.contains(">")) {
					predicateS = ">";
				} else if (s.contains("=")) {
					predicateS = "=";
				} else {
					fileBuffer.close();
					throw new Exception("Invalid predicate.");
				}
				oneConditon = s.split(predicateS);
				if (oneConditon.length<2) {
					fileBuffer.close();
					throw new Exception("Invalid value.");
				}
				columnName = oneConditon[0].trim();
				int columnIndex = table.getColumnIndex(columnName);
				if (columnIndex==-1) {
					fileBuffer.close();
					throw new Exception("No such column.");
				}
				switch (predicateS) {
				case "<=": predicate = SQLPredicate.LEQUAL; break;
				case ">=": predicate = SQLPredicate.GEQUAL; break;
				case "!=": predicate = SQLPredicate.NOTEQUAL; break;
				case "<": predicate = SQLPredicate.LESS; break;
				case ">": predicate = SQLPredicate.GREATER; break;
				default: predicate = SQLPredicate.EQUAL; break;
				}
				try {
					value = table.getColumns().get(columnIndex).getType().parse(oneConditon[1].trim());
				} catch (Exception e) {
					fileBuffer.close();
					throw new Exception("Invalid value.");
				}
				table.selectAnd(columnIndex,predicate,value);
			}
		}
		int cnt = 0;
		for (Record record : table) {
			record.remove();
			cnt++;
		}
		fileBuffer.close();
		if (cnt>1) {
			System.out.println(cnt + " lines affected.");
		} else {
			System.out.println(cnt + " line affected.");
		}
	} 
	private static void selectFrom (String cmd) throws Exception
	{
		String tableName;
		String[] conditions;
		String columnName;
		SQLPredicate predicate;
		SQLElement value;
		String[] oneConditon;
		String predicateS;
		tableName = cmd.split(" ")[0];
		File tableFile = new File(tableName);
		if (!tableFile.exists()) {
			throw new Exception("No such table.");
		}
		FileBuffer fileBuffer = new FileBuffer(tableFile);
		Table table = new Table(fileBuffer);
		cmd = cmd.substring(tableName.length(),cmd.length()).trim();
		if (cmd.contains("where")) {
			table.clearSelect();
			cmd = cmd.substring(5,cmd.length());
			conditions = cmd.split("and");
			for (int i = 0;i<conditions.length;i++) {
				conditions[i] = conditions[i].trim();
			}
			for (String s : conditions) {
				if (s.contains("<=")) {
					predicateS = "<=";
				} else if (s.contains(">=")) {
					predicateS = ">=";
				} else if (s.contains("!=")) {
					predicateS = "!=";
				} else if (s.contains("<")) {
					predicateS = "<";
				} else if (s.contains(">")) {
					predicateS = ">";
				} else if (s.contains("=")) {
					predicateS = "=";
				} else {
					fileBuffer.close();
					throw new Exception("Invalid predicate.");
				}
				oneConditon = s.split(predicateS);
				if (oneConditon.length<2) {
					fileBuffer.close();
					throw new Exception("Invalid value.");
				}
				columnName = oneConditon[0].trim();
				int columnIndex = table.getColumnIndex(columnName);
				if (columnIndex==-1) {
					fileBuffer.close();
					throw new Exception("No such column.");
				}
				switch (predicateS) {
				case "<=": predicate = SQLPredicate.LEQUAL; break;
				case ">=": predicate = SQLPredicate.GEQUAL; break;
				case "!=": predicate = SQLPredicate.NOTEQUAL; break;
				case "<": predicate = SQLPredicate.LESS; break;
				case ">": predicate = SQLPredicate.GREATER; break;
				default: predicate = SQLPredicate.EQUAL; break;
				}
				try {
					value = table.getColumns().get(columnIndex).getType().parse(oneConditon[1].trim());
				} catch (Exception e) {
					fileBuffer.close();
					throw new Exception("Invalid value.");
				}
				table.selectAnd(columnIndex,predicate,value);
			}
		}
		int cnt = 0;
		List<Column> columns = table.getColumns();
		int numOfColumns = columns.size();
		int[] columnWidth = new int[numOfColumns];
		int current = 0;
		for (int i=0;i<numOfColumns;i++) {
			columnWidth[i] = columns.get(i).getName().length();
		}
		for (Record record : table) {
			cnt++;
			for (int i=0;i<numOfColumns;i++) {
				current = record.get(i).toString().length();
				if (current > columnWidth[i]) {
					columnWidth[i] = current;
				}
			}
		}
		table.printHeader(columnWidth, System.out);
		for (Record record : table) {
			record.print(columnWidth, System.out);
		}
		table.printFoot(columnWidth,System.out);
		fileBuffer.close();
		if (cnt==0) {
			System.out.println("Empty result.");
		} else if (cnt>1){
			System.out.println(cnt + " lines affected.");
		} else {
			System.out.println(cnt + " line affected.");
		}
	}
}
