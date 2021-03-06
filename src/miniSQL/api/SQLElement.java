package miniSQL.api;

public abstract class SQLElement implements Comparable<SQLElement>, SQLSerializable<SQLElement>
{
	public abstract String toString();
	
	public abstract SQLElement parse(String raw) throws Exception;
}
