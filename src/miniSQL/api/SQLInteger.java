package miniSQL.api;

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
		block[offset++] = (byte)this.content;
		block[offset++] = (byte)(this.content >> 8);
		block[offset++] = (byte)(this.content >> 16);
		block[offset] = (byte)(this.content >> 24);
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
		int i;
		offset += 3;
		i = block[offset--] & 255;
		i = (i << 8) | (block[offset--] & 255);
		i = (i << 8) | (block[offset--] & 255);
		i = (i << 8) | (block[offset] & 255);
		return new SQLInteger(i);
	}

	@Override
	public SQLInteger parse(String raw) throws NumberFormatException
	{
		return new SQLInteger(Integer.parseInt(raw));
	}

}
