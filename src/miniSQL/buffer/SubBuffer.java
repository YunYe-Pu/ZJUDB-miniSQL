package miniSQL.buffer;

import miniSQL.api.SQLSerializable;

public class SubBuffer<T extends SQLSerializable<T>>
{
	public T read(int index, T obj)
	{
		//TODO
		return null;
	}
	
	public void write(int index, T obj)
	{
		//TODO
	}
	
	public int allocateBlock()
	{
		//TODO
		return 0;
	}
	
	public void removeBlock(int index)
	{
		//TODO
	}
	
	public int getMaxBlockIndex()
	{
		//TODO
		return 0;
	}
	
	public boolean isBlockValid(int index)
	{
		//TODO
		return false;
	}
}
