package miniSQL.api;

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
		int val = Float.floatToRawIntBits(this.content);
		block[offset++] = (byte)val;
		block[offset++] = (byte)(val >> 8);
		block[offset++] = (byte)(val >> 16);
		block[offset] = (byte)(val >> 24);
	}

	@Override
	public String toString()
	{
		return Float.toString(this.content);
	}

	@Override
	public SQLFloat read(byte[] block, int offset)
	{
		int bits;
		offset += 3;
		bits = block[offset--] & 255;
		bits = (bits << 8) | (block[offset--] & 255);
		bits = (bits << 8) | (block[offset--] & 255);
		bits = (bits << 8) | (block[offset] & 255);
		return new SQLFloat(Float.intBitsToFloat(bits));
	}

	@Override
	public SQLFloat parse(String raw) throws NumberFormatException
	{
		return new SQLFloat(Float.parseFloat(raw));
	}

}
