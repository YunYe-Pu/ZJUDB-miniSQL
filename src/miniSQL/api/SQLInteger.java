package miniSQL.api;

import miniSQL.IOHelper;

public class SQLInteger extends SQLElement
{
	public final int content;
	
	public SQLInteger(int content)
	{
		this.content = content;
	}
	
	public SQLInteger()
	{
		this(0);
	}
	
	@Override
	public int compareTo(SQLElement o)
	{
		if(!(o instanceof SQLInteger))
			return 0;
		else
			return this.content - ((SQLInteger)o).content;
	}

	@Override
	public int getSize()
	{
		return 4;
	}

	@Override
	public void write(byte[] block, int offset)
	{
		IOHelper.writeInteger(block, offset, this.content);
	}

	@Override
	public String toString()
	{
		return Integer.toString(this.content);
	}
	
	public int toInteger()
	{
		return this.content;
	}
	
	@Override
	public SQLInteger read(byte[] block, int offset)
	{
		return new SQLInteger(IOHelper.readInteger(block, offset));
	}

	@Override
	public SQLInteger parse(String raw) throws NumberFormatException
	{
		return new SQLInteger(Integer.parseInt(raw));
	}

}
