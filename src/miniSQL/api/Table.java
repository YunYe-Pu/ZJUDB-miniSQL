package miniSQL.api;

import java.util.Iterator;
import java.util.List;

import miniSQL.buffer.FileBuffer;

public class Table implements Iterable<Record>
{

	public Table(FileBuffer buffer)
	{
		
	}
	
	public void selectAnd(Column column, SQLPredicate predicate, SQLElement value)
	{
		//TODO API interface stub
	}
	public void selectOr(Column column, SQLPredicate predicate, SQLElement value)
	{
		//TODO API interface stub
	}
	
	public void clearSelect()
	{
		//TODO API interface stub
	}
	
	public void insert(Record record)
	{
		//TODO API interface stub
	}
	
	public List<Column> getColumns()
	{
		//TODO
		return null;
	}

	@Override
	public Iterator<Record> iterator()
	{
		// TODO Auto-generated method stub
		return null;
	}
	
	
//	public static Table open(String tableName) throws TableNotExistException
//	{
//		//TODO
//		return null;
//	}
//	
//	public static Table create(String tableName) throws TableAlreadyExistException
//	{
//		//TODO
//		return null;
//	}
}
