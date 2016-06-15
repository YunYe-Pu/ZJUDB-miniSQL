package miniSQL.api;

import java.io.PrintStream;
import java.util.List;

public class Record implements SQLSerializable<Record>
{
	private SQLElement[] elements;
	private Table owner;
	private int indexInBuffer;
	
	public Record(Table owner)
	{
		this.owner = owner;
		this.elements = new SQLElement[owner.getColumns().size()];
		this.indexInBuffer = -1;
	}
	
	public void set(int index, SQLElement value)
	{
		this.elements[index] = value;
	}
	
	public void parse(int index, String raw) throws Exception
	{
		this.elements[index] = this.owner.getColumns().get(index).getType().parse(raw);
	}
	
	public SQLElement get(int index)
	{
		return this.elements[index];
	}
	
	public SQLElement[] getElements()
	{
		return this.elements;
	}
	
	public int getIndexInBuffer()
	{
		return this.indexInBuffer;
	}
	
	public void setIndexInBuffer(int index)
	{
		this.indexInBuffer = index;
	}
	
	public boolean remove()
	{
		if(this.indexInBuffer < 0)
			return false;
		else
		{
			this.owner.getRecBuffer().removeEntry(this.indexInBuffer);
			List<Column> columns = this.owner.getColumns();
			for(int i = 0; i < columns.size(); i++)
			{
				BPlusTree t = columns.get(i).getIndex();
				if(t != null)
					t.deleteRecord(this.elements[i]);
			}
			this.indexInBuffer = -1;
			return true;
		}
	}
	
	public void print(int[] columnWidth, PrintStream output)
	{
		output.print('|');
		for(int i = 0; i < this.elements.length; i++)
			Table.printString(columnWidth[i], this.elements[i].toString(), output);
		output.println();
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
