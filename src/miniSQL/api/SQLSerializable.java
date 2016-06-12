package miniSQL.api;

public interface SQLSerializable
{
	public int getSize();
	
	public void write(byte[] block, int offset);
}
