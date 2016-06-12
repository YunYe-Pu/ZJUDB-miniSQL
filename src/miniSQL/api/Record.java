package miniSQL.api;

import java.util.ArrayList;
import java.util.List;

public class Record implements SQLSerializable<Record>
{
	
	private ArrayList<SQLElement> elements;
	private List<Column> schema;
	
	public Record(List<Column> schema)
	{
		this.schema = schema;
		this.elements = new ArrayList<>(this.schema.size());
	}
	
	public void set(int index, SQLElement value)
	{
		this.elements.set(index, value);
	}
	
	

	@Override
	public int getSize()
	{
		int size = 0;
		for(SQLElement e : this.elements)
			size += e.getSize();
		return size;
	}

	@Override
	public void write(byte[] block, int offset)
	{
		for(SQLElement e : this.elements)
		{
			e.write(block, offset);
			offset += e.getSize();
		}
	}

	@Override
	public Record read(byte[] block, int offset)
	{
		Record ret = new Record(this.schema);
		int i = 0;
		for(Column c : this.schema)
		{
			ret.set(i++, c.getType().read(block, offset));
			offset += c.getType().getSize();
		}
		return ret;
	}
	
}
