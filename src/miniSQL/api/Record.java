package miniSQL.api;

import java.util.ArrayList;
import java.util.List;

public class Record implements SQLSerializable<Record>
{
	private ArrayList<SQLElement> elements;
	private Table owner;
	private int index;
	
	public Record(Table owner)
	{
		this.owner = owner;
		this.elements = new ArrayList<>(owner.getColumns().size());
		this.index = -1;
	}
	
	public void set(int index, SQLElement value)
	{
		this.elements.set(index, value);
	}
	
	public SQLElement get(int index)
	{
		return this.elements.get(index);
	}
	
	public List<SQLElement> getElements()
	{
		return this.elements;
	}
	
	public int getRecIndex()
	{
		return this.index;
	}
	
	public boolean remove()
	{
		if(this.index < 0)
			return false;
		else
		{
			this.owner.getRecBuffer().removeBlock(this.index);
			List<Column> columns = this.owner.getColumns();
			for(int i = 0; i < columns.size(); i++)
			{
				BPlusTree t = columns.get(i).getIndex();
				if(t != null)
					t.deleteRecord(this.elements.get(i));
			}
			return true;
		}
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
		Record ret = new Record(this.owner);
		int i = 0;
		for(Column c : this.owner.getColumns())
		{
			ret.set(i++, c.getType().read(block, offset));
			offset += c.getType().getSize();
		}
		return ret;
	}
	
}
