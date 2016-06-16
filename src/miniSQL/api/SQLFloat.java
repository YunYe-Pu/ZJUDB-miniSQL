package miniSQL.api;

import miniSQL.IOHelper;

public class SQLFloat extends SQLElement
{
	public final float content;

	public SQLFloat(float content)
	{
		this.content = content;
	}
	
	public SQLFloat()
	{
		this(0);
	}
	
	public SQLFloat(SQLInteger o)
	{
		this(o.content);
	}
	
	@Override
	public int compareTo(SQLElement o)
	{
		if(!(o instanceof SQLFloat))
			return 0;
		else
			return Float.compare(this.content, ((SQLFloat)o).content);
	}

	@Override
	public int getSize()
	{
		return 4;
	}

	@Override
	public void write(byte[] block, int offset)
	{
		IOHelper.writeFloat(block, offset, this.content);
	}

	@Override
	public String toString()
	{
		return Float.toString(this.content);
	}
	
	public float toFloat()
	{
		return this.content;
	}

	@Override
	public SQLFloat read(byte[] block, int offset)
	{
		return new SQLFloat(IOHelper.readFloat(block, offset));
	}

	@Override
	public SQLFloat parse(String raw) throws NumberFormatException
	{
		return new SQLFloat(Float.parseFloat(raw));
	}

}
