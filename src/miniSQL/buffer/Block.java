package miniSQL.buffer;

// here to assign values:
// Byte[BlockSize - 4] int : block index
// Byte[] 

public class Block {
	public static final int BlockSize = 8192;
	
	public byte[] val;
	public int index;
	public int age;
	
	public int getInt(int offset){
		return   val[offset + 3] & 0xFF |  
	            (val[offset + 2] & 0xFF) << 8 |  
	            (val[offset + 1] & 0xFF) << 16 |  
	            (val[offset] & 0xFF) << 24;  
	}
	
	public void setInt(int offset, int i){
		val[offset] = (byte) ((i >> 24) & 0xFF);
		val[offset + 1] = (byte) ((i >> 16) & 0xFF);  
		val[offset + 2] = (byte) ((i >> 8) & 0xFF);   
		val[offset + 3] = (byte) (i & 0xFF);
	}
	
	public Block(int index, byte[] val){
		this.val = val;
	}
	
	public Block(int index){
		this.index = index;
		val = new byte[BlockSize];
	}
	
	public Block(int index, int age){
		this.index =index;
		this.age = age;
		val = new byte[BlockSize];
	}
	
	public byte[] close(){
		return val;
	}
}
