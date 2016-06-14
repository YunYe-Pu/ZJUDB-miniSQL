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
			block[i + offset] = 0;
	}

	@Override
	public String toString()
	{
		return this.content;
	}

	@Override
	public SQLString read(byte[] block, int offset)
	{
		int i;
		for(i = offset; i < this.length && block[i] != 0; i++);
		char[] data = new char[i];
		for(i = 0; i < data.length; i++)
			data[i] = (char)block[i + offset];
		return new SQLString(String.valueOf(data), this.length);
	}

	@Override
	public SQLString parse(String raw) throws Exception
	{
		char[] c1 = raw.toCharArray();
		boolean inEscape = false;
		if(c1[0] != '\'')
			throw new Exception("Incorrect string format in string " + raw + ".");
		int i, j;
		for(i = 1, j = 0; i < c1.length; i++)
		{
			if(inEscape)
			{
				switch(c1[i]) {
				case 'b':
					c1[j++] = '\b';
					break;
				case 't':
					c1[j++] = '\t';
					break;
				case 'n':
					c1[j++] = '\n';
					break;
				case 'f':
					c1[j++] = '\f';
					break;
				case 'r':
					c1[j++] = '\r';
					break;
				case '\"':
					c1[j++] = '\"';
					break;
				case '\'':
					c1[j++] = '\'';
					break;
				case '\\':
					c1[j++] = '\\';
					break;
				default:
					c1[j++] = '\\';
					c1[j++] = c1[i];
					break;
				}
				inEscape = false;
			}
			else if(c1[i] == '\\')
				inEscape = true;
			else if(c1[i] == '\'')
				break;
			else
				c1[j++] = c1[i];
		}
		if(i < c1.length - 1)
			throw new Exception("Incorrect string format in string " + raw + ".");
		char[] c2 = new char[j];
		for(i = 0; i < j; i++)
			c2[i] = c1[i];
		return new SQLString(String.valueOf(c2), this.length);
	}
}
