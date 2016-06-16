package miniSQL.api;

import miniSQL.IOHelper;
import miniSQL.api.BPlusTree.BPlusTreeNode;
import miniSQL.buffer.SubBuffer;

public class Column implements SQLSerializable<Column>
{
	private boolean unique;
	private boolean primary;
	
	private int indexSubBuffer = -1;
	private BPlusTree index = null;
	
	private SQLElement type;
	private String name;
	private Table owner = null;
	private int indexInOwner = -1;
	
	public Column(String name, SQLElement type, boolean primary, boolean unique)
	{
		this.name = name;
		this.type = type;
		this.unique = unique;
		this.primary = primary;
		if(primary)
			this.unique = true;
	}
	
	protected Column(Table owner)
	{
		this.owner = owner;
		this.name = "";
		this.type = null;
	}
	
	protected void setOwner(Table owner, int index)
	{
		this.owner = owner;
		this.indexInOwner = index;
	}
	
	protected int getIndexInOwner()
	{
		return this.indexInOwner;
	}
	
	public boolean isIndexed()
	{
		return this.indexSubBuffer >= 0;
	}
	
	public boolean isPrimary()
	{
		return this.primary;
	}
	
	public boolean isUnique()
	{
		return this.unique;
	}
	
	public String getName()
	{
		return this.name;
	}
	
	public SQLElement getType()
	{
		return this.type;
	}
	
	public void createIndex()
	{
		if(this.indexSubBuffer >= 0 || !this.unique)
			return;
		this.indexSubBuffer = this.owner.getBuffer().createSubBuffer(20 + 3 * this.type.getSize());
		SubBuffer<BPlusTreeNode> subBuffer = this.owner.getBuffer().getSubBuffer(this.indexSubBuffer);
		this.index = new BPlusTree(subBuffer, this.type, -1, -1);
		this.owner.clearSelect();
		for(Record rec : this.owner)
			this.index.insertRecord(rec.get(this.indexInOwner), rec.getIndexInBuffer());
		this.owner.getColumnBuffer().write(this.indexInOwner, this);
	}
	
	public void dropIndex()
	{
		if(this.indexSubBuffer < 0 || this.primary)
			return;
		this.owner.getBuffer().deleteSubBuffer(this.indexSubBuffer);
		this.index = null;
		this.indexSubBuffer = -1;
		this.owner.getColumnBuffer().write(this.indexInOwner, this);
	}
	
	protected BPlusTree getIndex()
	{
		return this.index;
	}

	
	//Data: column name(32), index sub-buffer(-1 for not indexed)(4), type attr.(4),
	//type attr.(4), index root(4), index height(4), {is primary, is unique, type}(1)
	@Override
	public int getSize()
	{
		return 64;
	}

	@Override
	public void write(byte[] block, int offset)
	{
		IOHelper.writeString(block, offset, this.name, 32);
		offset += 32;
		IOHelper.writeInteger(block, offset, this.indexSubBuffer);
		IOHelper.writeInteger(block, offset + 4, this.type.getSize());
		if(this.index != null)
		{
			IOHelper.writeInteger(block, offset + 8, this.index.getRoot());
			IOHelper.writeInteger(block, offset + 12, this.index.getHeight());
		}
		else
		{
			IOHelper.writeInteger(block, offset + 8, -1);
			IOHelper.writeInteger(block, offset + 12, -1);
		}
		byte flags = 0;
		if(this.primary) flags |= 0x80;
		if(this.unique) flags |= 0x40;
		if(this.type instanceof SQLInteger)
			flags |= 1;
		else if(this.type instanceof SQLFloat)
			flags |= 2;
		block[offset + 16] = flags;
	}

	@Override
	public Column read(byte[] block, int offset)
	{
		Column ret = new Column(this.owner);
		ret.name = IOHelper.readString(block, offset, 32);
		offset += 32;
		ret.indexSubBuffer = IOHelper.readInteger(block, offset);
		int typeAttr = IOHelper.readInteger(block, offset + 4);
		int indexRoot = IOHelper.readInteger(block, offset + 8);
		int indexHeight = IOHelper.readInteger(block, offset + 12);
		byte flags = block[offset + 16];
		ret.primary = (flags & 0x80) != 0;
		ret.unique = (flags & 0x40) != 0;
		flags &= 0x3f;
		switch(flags) {
		case 0:
			ret.type = new SQLString(typeAttr);
			break;
		case 1:
			ret.type = new SQLInteger();
			break;
		case 2:
			ret.type = new SQLFloat();
			break;
		}
		if(ret.indexSubBuffer >= 0)
		{
			SubBuffer<BPlusTreeNode> subBuffer = ret.owner.getBuffer().getSubBuffer(ret.indexSubBuffer);
			ret.index = new BPlusTree(subBuffer, ret.type, indexHeight, indexRoot);
		}
		return ret;
	}

}
