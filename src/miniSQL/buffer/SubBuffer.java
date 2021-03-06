package miniSQL.buffer;

import java.util.ArrayList;
import java.util.LinkedList;

import miniSQL.api.SQLSerializable;

public class SubBuffer<T extends SQLSerializable<T>>
{
	FileBuffer filebuf;
	int index;
	int size;
	int EntryCount;
	int hblockIndex;
	ArrayList<Integer> blocksIndex = new ArrayList<>();
	LinkedList<Integer> emptyEntryIndex = new LinkedList<>();
	int MaxEntryIndex;
	
	private Object[] entryPool = new Object[32];
	private int[] entryPoolIndex = new int[32];
	
	public String toString(){
		String s = new String("blocks are:");
		for(int i = 0; blocksIndex.size() > i && blocksIndex.get(i) != 0; i++){
			s = s + blocksIndex.get(i) + " ";
		}
		s = s + " and ";
		s = s + "maxentry are:" + MaxEntryIndex + "\n";
		return s;
	}
	
	public SubBuffer(FileBuffer filebuf, int index, int size){
		//System.out.println("SubBuf " + index + " Created");
		this.filebuf = filebuf;
		this.index = index;
		this.size = size;
		EntryCount = (Block.BlockSize) / (size + 1);
		hblockIndex = filebuf.createNewBlock();
		MaxEntryIndex = -1;
		
		for(int i = 0; i < 32; i++)
			entryPoolIndex[i] = -1;
	}	
	
	//read in a subbuffer according to the existing file
	public SubBuffer(FileBuffer filebuf, int index, int size, int hblockindex){
		this.filebuf = filebuf;
		this.index = index;
		this.size = size;
		EntryCount = (Block.BlockSize) / (size + 1);
		this.hblockIndex = hblockindex;
		Block hblock = filebuf.getBlock(hblockIndex);
		MaxEntryIndex = hblock.getInt(0);
		int i, j;
		for(i = 0; i <= MaxEntryIndex/EntryCount && MaxEntryIndex >= 0; i++){
			blocksIndex.add(hblock.getInt((i+1) * 4));
		}
		i = 0;
		for(int blockindex : blocksIndex){
			if(blockindex == 0) break;
			for(j = 0; j < EntryCount; j++){
				if(i * EntryCount + j < MaxEntryIndex && filebuf.getBlock(blockindex).val[Block.BlockSize - EntryCount + j] == (byte) 0){
					emptyEntryIndex.add(i * EntryCount + j);
				}
			}
			i++;
		}

		for(i = 0; i < 32; i++)
			entryPoolIndex[i] = -1;
	}	
	
	public void onDelete(){
		filebuf.removeBlock(hblockIndex);
		for(int blockindex : blocksIndex){
			if(blockindex == 0) break;
			filebuf.removeBlock(blockindex);
		}
	}
	
	public void close(){
		int i;
		Block hblock = filebuf.getBlock(hblockIndex);
		hblock.setInt(0, MaxEntryIndex);
		//System.out.println("0" + " |&| " + blocksIndex.size()); 
		for(i = 0; blocksIndex.size() > i && blocksIndex.get(i) != 0; i++){
			//System.out.println("blockindex " + blocksIndex.get(i) + " written");
			hblock.setInt((i + 1) * 4, blocksIndex.get(i));
		}
	}
	
	public T read(int index, T obj)
	{
		if(entryPoolIndex[index & 31] == index)
			return (T)entryPool[index & 31];
		Block b = filebuf.getBlock(blocksIndex.get(index/EntryCount));
		filebuf.ageBlockQueue.remove(b);
		b.age = filebuf.curage++;
		filebuf.ageBlockQueue.add(b);
		int entryinblock = index % EntryCount;
		T ret = obj.read(b.val, entryinblock * size);
		entryPoolIndex[index & 31] = index;
		entryPool[index & 31] = ret;
		return ret;
	}
	
	public void write(int index, T obj)
	{
		Block b = filebuf.getBlock(blocksIndex.get(index/EntryCount));
		filebuf.ageBlockQueue.remove(b);
		b.age = filebuf.curage++;
		filebuf.ageBlockQueue.add(b);
		int entryinblock = index % EntryCount;
		obj.write(b.val, entryinblock * size);
		entryPool[index & 31] = obj;
		entryPoolIndex[index & 31] = index;
	}
	
	public int allocateEntry()
	{
		int entryindex, i;
		while(true){
			if(emptyEntryIndex.isEmpty()){
				entryindex = ++MaxEntryIndex;
				break;
			}else{
			i = emptyEntryIndex.poll();
				if(i < MaxEntryIndex){
					entryindex = i;
					break;
				}
			}
			//if(index == 1) System.out.println("allo to sub1 with" + i);
		}
		if(entryindex/EntryCount + 1 > blocksIndex.size()){
			for(i = blocksIndex.size(); i < entryindex/EntryCount + 1; i++){			
				int blockindex = filebuf.createNewBlock();
				blocksIndex.add(blockindex);
			}
		}
		//System.out.println("Sub "+ index + " Entry " + entryindex + " Created");
		setEntryState(entryindex, (byte)1);
		return entryindex;
	}

	public void removeEntry(int index)
	{
		int blocknumber = index/EntryCount;
		Block b = filebuf.getBlock(blocksIndex.get(index/EntryCount));
		int entryinblock = index % EntryCount;
		setEntryState(index, (byte)0);
		
		if(index < MaxEntryIndex){
			emptyEntryIndex.add(index);
		}else{	
			int i = entryinblock;
			while(b.val[Block.BlockSize - EntryCount + i] == 0){
				MaxEntryIndex--;
				if(i == 0){
					filebuf.removeBlock(blocksIndex.get(blocknumber));
					blocksIndex.remove(blocknumber);
					if(blocknumber == 0)break;
					b = filebuf.getBlock(blocksIndex.get(blocknumber--));
					i = EntryCount - 1;
				}else{
					i--;
				}
			}
		}
	}
	
	public int getMaxEntryIndex()
	{
		return MaxEntryIndex + 1;
	}
	
	public boolean isEntryValid(int index)
	{
		if(index/EntryCount >= blocksIndex.size()) return false;
		Block b = filebuf.getBlock(blocksIndex.get(index/EntryCount));
		int entryinblock = index % EntryCount;
		if(b.val[Block.BlockSize - EntryCount + entryinblock] == (byte)1){
			return true;
		}else{
			return false;
		}
	}	
	
	private void setEntryState(int entryindex, byte state){
		Block b = filebuf.getBlock(blocksIndex.get(entryindex/EntryCount));
		int entryinblock = entryindex % EntryCount;
		b.val[Block.BlockSize - EntryCount + entryinblock] = state;
	}
}
