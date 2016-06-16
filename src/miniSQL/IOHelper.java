package miniSQL;

public final class IOHelper
{
	private IOHelper() { }
	
	public static int readInteger(byte[] block, int offset)
	{
		int i;
		i = block[offset + 3] & 255;
		i = (i << 8) | (block[offset + 2] & 255);
		i = (i << 8) | (block[offset + 1] & 255);
		i = (i << 8) | (block[offset] & 255);
		return i;
	}
	
	public static void writeInteger(byte[] block, int offset, int value)
	{
		block[offset] = (byte)value;
		block[offset + 1] = (byte)(value >> 8);
		block[offset + 2] = (byte)(value >> 16);
		block[offset + 3] = (byte)(value >> 24);
	}
	
	public static float readFloat(byte[] block, int offset)
	{
		return Float.intBitsToFloat(readInteger(block, offset));
	}
	
	public static void writeFloat(byte[] block, int offset, float value)
	{
		writeInteger(block, offset, Float.floatToRawIntBits(value));
	}
	
	public static String readString(byte[] block, int offset, int maxLength)
	{
		if(maxLength < 0) maxLength = block.length - offset;
		int i;
		for(i = 0; i < maxLength && block[i + offset] != 0; i++);
		char[] data = new char[i];
		for(i = 0; i < data.length; i++)
			data[i] = (char)block[i + offset];
		return String.valueOf(data);
	}
	
	public static void writeString(byte[] block, int offset, String value, int maxLength)
	{
		if(maxLength < 0) maxLength = value.length();
		byte[] data = value.getBytes();
		int i;
		for(i = 0; i < maxLength && i < data.length; i++)
			block[i + offset] = data[i];
		for(; i < maxLength; i++)
			block[i + offset] = 0;
	}
}
