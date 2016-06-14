package miniSQL.buffer;

import java.io.File;

import miniSQL.api.SQLSerializable;

public class FileBuffer
{
	public FileBuffer(String tableFileName)
	{
		
	}
	public FileBuffer(File tableFile) 
	{
		
	}
	public void close()
	{
		
	}
	public byte[] getBlock(int blockOffset)
	{
		//TODO
		return null;
	}
	
	public <T> void write(int byteOffset, SQLSerializable<T> content)
	{
		//TODO
	}
	
	public int createSubBuffer(int size)
	{
		//TODO
		return 0;
	}
	
	public boolean isSubBufferValid(int index)
	{
		//TODO
		return false;
	}
	
	public <T extends SQLSerializable<T>> SubBuffer<T> getSubBuffer(int index)
	{
		//TODO
		return null;
	}
	
	public void deleteSubBuffer(int index)
	{
		//TODO
	}
}
