package miniSQL.api;

public interface SQLSerializable<T>
{
	public int getSize();
	
	public void write(byte[] block, int offset);
	
	public T read(byte[] block, int offset);
}
