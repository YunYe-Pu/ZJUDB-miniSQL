package miniSQL.api;

public class SQLString extends SQLElement
{
	public final String content;
	public final int length;
	
	public SQLString(String content, int length)
	{
		this.content = content;
		this.length = length;
	}
	
	public SQLString(int length)
	{
		this("", length);
	}
	
	@Override
	public int compareTo(SQLElement o)
	{
		if(!(o instanceof SQLString))
			return 0;
		else
			return this.content.compareTo(((SQLString)o).content);
	}

	@Override
	public int getSize()
	{
		return this.length;
	}

	@Override
	public void write(byte[] block, int offset)
	{
		byte[] data = this.content.getBytes();
		int i;
		for(i = 0; i < this.length && i < data.length; i++)
			block[i + offset] = data[i];
		for(; i < this.length; i++)
			block[i + offset] = (byte)0;
	}

	@Override
	public String toString()
	{
		return this.content;
	}

	@Override
	public SQLString read(byte[] block, int offset)
	{
		char[] data = new char[this.length];
		int i;
		for(i = 0; i < this.length; i++)
			data[i] = (char)block[i + offset];
		return new SQLString(String.valueOf(data), this.length);
	}

	@Override
	public SQLString parse(String raw)
	{
		return new SQLString(raw, this.length);
	}
	
}
