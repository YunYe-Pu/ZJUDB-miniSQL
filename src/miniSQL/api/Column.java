package miniSQL.api;

public class Column
{
	private boolean indexed;
	private boolean unique;
	
	private SQLElement type;
	private String name;
	
	public Column(String name, SQLElement type)
	{
		this.name = name;
		this.type = type;
	}
	
	public boolean isIndexed()
	{
		return this.indexed;
	}
	
	public boolean isUnique()
	{
		return this.unique;
	}
	
	public String getName()
	{
		return this.name;
	}
	
	public SQLElement getType()
	{
		return this.type;
	}
	
	public void createIndex()
	{
		//TODO
	}
	
	public void dropIndex()
	{
		//TODO
	}
}
