package miniSQL.buffer;

import java.io.*;
import java.util.*;

import miniSQL.api.SQLSerializable;


public class FileBuffer
{
	public static final int BlockCount = 64;
	
	File f;
	
	HashMap<Integer, Block> blocks = new HashMap<Integer, Block>(BlockCount);
	HashMap<Integer, SubBuffer> subbufs = new HashMap<Integer, SubBuffer>();
	LinkedList<Integer> emptyBlockIndex = new LinkedList<Integer>();
	LinkedList<Integer> emptySubbufIndex = new LinkedList<Integer>();
	PriorityQueue<Block> ageBlockQueue = new PriorityQueue<Block>(new Comparator<Block>(){
		 public int compare(Block b1, Block b2){
			 if(b1.age< b2.age){
				 return -1;
			 }else if(b1.age == b2.age){
				 return 0;
			 }else{
				 return 1;
			 }
		 }
	 }); 
	int curage = 0;
	int MaxBlockIndex;
	int MaxSubbufIndex;
	
	public FileBuffer(File f){
		this.f = f;
		try{
			if(!f.exists()){
				f.createNewFile();
				addBlock(0, new Block(0, curage++));
				addBlock(1, new Block(1, curage++));
				addBlock(2, new Block(2, curage++));
				MaxBlockIndex = 2;
				MaxSubbufIndex = -1;
			}else{
				Block b = getBlock(0);
				int i = 0;
				int subbufindex, hblockindex, size;
				while(true){
					subbufindex = b.getInt(i * 12);
					hblockindex = b.getInt(i * 12 + 4);
					size = b.getInt(i * 12 + 8);
					if(subbufindex < 0) break;
					subbufs.put(subbufindex, new SubBuffer(this, subbufindex, size, hblockindex));
					i++;
				}
				
				b = getBlock(1);
				int blockindex;
				MaxBlockIndex = b.getInt(0);
				i = 0;
				while(true){
					blockindex = b.getInt(i * 4 + 4);
					if(blockindex < 0) break;
					emptyBlockIndex.add(blockindex);
					i++;
				}
				
				b = getBlock(2);
				MaxSubbufIndex = b.getInt(0);
				i = 0;
				while(true){
					subbufindex = b.getInt(i * 4 + 4);
					if(subbufindex < 0) break;
					emptySubbufIndex.add(subbufindex);
					i++;
				}
			}
		}catch(IOException ioe){
			ioe.printStackTrace();
		}
	}
	
	public void close(){
		int i = 0;
		Block b = getBlock(0);
		Iterator<Map.Entry<Integer, SubBuffer>> iterSubBuf = subbufs.entrySet().iterator();
		while(iterSubBuf.hasNext()){
			Map.Entry<Integer, SubBuffer> entry = iterSubBuf.next();
			SubBuffer subbuf = entry.getValue();
			int subbufindex = entry.getKey();
			b.setInt(i*12, subbufindex);
			b.setInt(i*12 + 4, subbuf.hblockIndex);
			b.setInt(i*12 + 8, subbuf.size);
			i++;
		}
		b.setInt(i*12, -1);
		b.setInt(i*12 + 4, -1);
		b.setInt(i*12 + 8, -1);
		
		b = getBlock(1);
		b.setInt(0, MaxBlockIndex);
		i = 0;
		while(!emptyBlockIndex.isEmpty()){
			b.setInt(i*4 + 4, emptyBlockIndex.poll());
			i++;
		}
		b.setInt(i*4 + 4, -1);
		
		b = getBlock(2);
		b.setInt(0, MaxSubbufIndex);
		i = 0;
		while(!emptySubbufIndex.isEmpty()){
			b.setInt(i*4 + 4, emptySubbufIndex.poll());
			i++;
		}
		b.setInt(i*4 + 4, -1);
		
		Iterator<Map.Entry<Integer, Block>> iterBlock = blocks.entrySet().iterator();
		while(iterBlock.hasNext()){
			Map.Entry<Integer, Block> entry =  iterBlock.next();
			int blockindex = entry.getKey();
			writebackBlock(blockindex);
		}
	}
	/*not uesd here
	public byte[] getBlock(int blockOffset)
	{
		//TODO
		return null;
	}
	
	public <T> void write(int byteOffset, SQLSerializable<T> content)
	{
		//TODO
	}
	*/
	public int createSubBuffer(int size)
	{
		int i;
		if(emptySubbufIndex.isEmpty()){
			i = ++MaxSubbufIndex;
		}else{
			i = emptySubbufIndex.poll();
		}
		subbufs.put(i ,new SubBuffer(this, i, size));
		return i;
	}
	
	public <T extends SQLSerializable<T>> SubBuffer<T> getSubBuffer(int index)
	{
		return subbufs.get(index);
	}
	
	public boolean isSubBufferValid(int index)
	{
		return subbufs.containsKey(index);
	}
	
	public void deleteSubBuffer(int index)
	{
		emptySubbufIndex.add(index);
		subbufs.get(index).onDelete();
	}
	
	public int createNewBlock(){
		int i;
		if(emptyBlockIndex.isEmpty()){
			i = ++MaxBlockIndex;
		}else{
			i = emptyBlockIndex.poll();
		}
		Block b = new Block(i, curage++);
		addBlock(i, b);
		return i;
	}	
	
	// 
	public void addBlock(int index, Block block){
		if(blocks.size() > 64){
			int oldi = ageBlockQueue.poll().index;
			writebackBlock(oldi);
			blocks.remove(oldi);
		}
		ageBlockQueue.add(block);
		blocks.put(index, block);
	}
	
	public Block getBlock(int index){
		Block block;
		if(!blocks.containsKey(index)){
			byte[] val = new byte[Block.BlockSize];
			try{
				RandomAccessFile raf = new RandomAccessFile(f, "r");
				raf.seek(Block.BlockSize * index);
				raf.read(val, 0, Block.BlockSize);
				raf.close();	
			}catch(IOException ioe){
				ioe.printStackTrace();
			}
			block = new Block(index, val);
			addBlock(index, block);
		}else{
			block = blocks.get(index);
		}
		return block;
	}
	
	private void writebackBlock(int index){
		byte[] val = blocks.get(index).close();
		try{
			RandomAccessFile raf = new RandomAccessFile(f, "rw");
			raf.seek(Block.BlockSize * index);
			raf.write(val);
			raf.close();
		}catch(IOException ioe){
			ioe.printStackTrace();
		}
	}
	
	// totally delete the block, not write back
	public void removeBlock(int index){
		Block b = blocks.get(index);
		blocks.remove(index);
		emptyBlockIndex.add(index);
		ageBlockQueue.remove(b);
	}
	
	/*
	public static void main(String[] args){
		class Datatype implements SQLSerializable<Datatype>{
			int size;
			byte[] val;
			
			public Datatype(int size){
				this.size = size;
				val = new byte[size];
			}
			
			public int getSize(){
				return size;
			}
			
			public void write(byte[] block, int offset){
				System.arraycopy(val, 0, block, offset, size);
			}
			
			public Datatype read(byte[] block, int offset){
				Datatype d = new Datatype(size);
				System.arraycopy(block, offset, d.val, 0, size);
				return d;
			}
		}
		File f;
		int size = 10;
		f = new File("E://test.msq");
		FileBuffer nfb = new FileBuffer(f);
		SubBuffer subbuf;
		Datatype rawdata = new Datatype(size);
		rawdata.val[0] = 1;
		rawdata.val[1] = 2;
		int index, i;
		index = nfb.createSubBuffer(size);
		for(i = 0; i < 100; i++){
			subbuf = nfb.getSubBuffer(index);
			subbuf.write(subbuf.allocateEntry(), rawdata);
		}
		rawdata.val[1] = 2;
		nfb.close();
	}
	*/
}
