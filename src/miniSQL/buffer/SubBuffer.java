package miniSQL.buffer;

import miniSQL.api.SQLSerializable;

public class SubBuffer<T extends SQLSerializable<T>>
{
	FileBuffer filebuf;
	
	public T read(int index, T obj)
	{
		//TODO
		return null;
	}
	
	public void write(int index, T obj)
	{
		//TODO
	}
	
	public int allocateEntry()
	{
		//TODO
		return 0;
	}
	
	public void removeEntry(int index)
	{
		//TODO
	}
	
	public int getMaxEntryIndex()
	{
		//TODO
		return 0;
	}
	
	public boolean isEntryValid(int index)
	{
		//TODO
		return false;
	}
}
