package miniSQL.api;

import miniSQL.buffer.SubBuffer;

public class BPlusTree
{
//	protected void linearSelect(Selection selection, BPlusTreeNode start, int index, Predicate<SQLElement> until)
//	{
//		if(start.keyCnt >= 1 && index <= 0)
//			if(until.test(start.key0))
//				selection.select(start.ptr0);
//		if(start.keyCnt >= 2 && index <= 1)
//			if(until.test(start.key1))
//				selection.select(start.ptr1);
//		if(start.keyCnt >= 3 && index <= 2)
//			if(until.test(start.key2))
//				selection.select(start.ptr2);
//		while(start.ptr3 >= 0)
//		{
//			start = this.buffer.read(start.ptr3, this.rootNode);
//			if(start.keyCnt >= 1)
//			{
//				if(until.test(start.key0))
//					selection.select(start.ptr0);
//				else
//					break;
//			}
//			if(start.keyCnt >= 2)
//			{
//				if(until.test(start.key1))
//					selection.select(start.ptr1);
//				else
//					break;
//			}
//			if(start.keyCnt >= 3)
//			{
//				if(until.test(start.key2))
//					selection.select(start.ptr2);
//				else
//					break;
//			}
//		}
//	}
	
	public BPlusTreeNode root;
	private SubBuffer<BPlusTreeNode> buffer;
	public int height;
	
	//The stack used to record the find operation
	private BPlusTreeNode[] nodeStack = new BPlusTreeNode[20];
	private int[] pathStack = new int[20];
	private int stackPtr = 0;
	
	public BPlusTree(SubBuffer<BPlusTreeNode> buffer, SQLElement keyType, int height, int rootIndex)
	{
		BPlusTreeNode dummy = new BPlusTreeNode(keyType);
		if(height >= 0)
		{
			this.root = buffer.read(rootIndex, dummy);
			this.root.index = rootIndex;
		}
		else
			this.root = dummy;
		this.buffer = buffer;
		this.height = height;
	}
	
	public int getHeight()
	{
		return this.height;
	}
	
	public int getRoot()
	{
		return this.root.index;
	}
	
	public int findRecord(SQLElement key)
	{
		if(this.find(key))
		{
			int i = this.stackPtr - 1;
			return this.nodeStack[i].ptr[this.pathStack[i]];
		}
		else
			return -1;
	}
	
	public IntIterator getIterator()
	{
		int i = this.stackPtr - 1;
		return new IntIterator(this.nodeStack[i], this.pathStack[i]);
	}

	public boolean insertRecord(SQLElement key, int index)
	{
		if(this.find(key))
			return false;
		if(this.height < 0)
		{
			this.root.index = this.buffer.allocateEntry();
			this.root.ptr[0] = index;
			this.root.key[0] = key;
			this.root.ptr[1] = -1;
			this.root.keyCnt = 1;
			this.buffer.write(this.root.index, this.root);
			this.height = 0;
			return true;
		}
		BPlusTreeNode newNode = null;
		SQLElement medianKey = null;
		BPlusTreeNode currNode;
		int currPath;
		int newIndex = -1;

		//Insert/split leaf
		this.stackPtr--;
		currNode = this.nodeStack[this.stackPtr];
		currPath = this.pathStack[this.stackPtr];
		if(currNode.keyCnt >= 3)
		{
			newNode = new BPlusTreeNode(this.root.keyType);
			newNode.index = this.buffer.allocateEntry();
			int i, j;
			for(i = 3, j = 2; i >= 0; i--)
			{
				BPlusTreeNode n = i >= 2? newNode: currNode;
				if(i == currPath)
				{
					n.key[i & 1] = key;
					n.ptr[i & 1] = index;
				}
				else
				{
					n.key[i & 1] = currNode.key[j];
					n.ptr[i & 1] = currNode.ptr[j];
					j--;
				}
			}
			currNode.ptr[2] = -1;
			currNode.key[2] = null;
			currNode.keyCnt = 2;
			newNode.keyCnt = 2;
			//link
			if(currNode.ptr[3] >= 0)
			{
				BPlusTreeNode nextNode = this.buffer.read(currNode.ptr[3], this.root);
				nextNode.prev = newNode.index;
				this.buffer.write(nextNode.index, nextNode);
			}
			newNode.ptr[3] = currNode.ptr[3];
			currNode.ptr[3] = newNode.index;
			newNode.prev = currNode.index;
			this.buffer.write(currNode.index, currNode);
			this.buffer.write(newNode.index, newNode);
			medianKey = newNode.key[0];
			newIndex = newNode.index;
		}
		else
		{
			for(int i = 1; i >= currPath; i--)
			{
				currNode.ptr[i + 1] = currNode.ptr[i];
				currNode.key[i + 1] = currNode.key[i];
			}
			currNode.key[currPath] = key;
			currNode.ptr[currPath] = index;
			currNode.keyCnt++;
			this.buffer.write(currNode.index, currNode);
		}
		
		//Cascade split
		while(newIndex >= 0 && this.stackPtr > 0)
		{
			this.stackPtr--;
			currNode = this.nodeStack[this.stackPtr];
			SQLElement nextMedianKey = null;
			int nextIndex = -1;
			currPath = this.pathStack[this.stackPtr];
			if(currNode.keyCnt == 4)
			{
				newNode = new BPlusTreeNode(this.root.keyType);
				newNode.index = this.buffer.allocateEntry();
				nextMedianKey = currNode.key[1];
				newNode.key[0] = currNode.key[2];
				newNode.ptr[0] = currNode.ptr[2];
				newNode.ptr[1] = currNode.ptr[3];
				newNode.keyCnt = 2;
				currNode.keyCnt = 2;
				currNode.ptr[2] = currNode.ptr[3] = -1;
				currNode.key[2] = null;
				nextIndex = newNode.index;
				if(currPath >= 2)
				{
					this.buffer.write(currNode.index, currNode);
					currNode = newNode;
				}
				else
					this.buffer.write(newNode.index, newNode);
				currPath &= 1;
			}
			for(int i = 1; i >= currPath; i--)
			{
				currNode.ptr[i + 2] = currNode.ptr[i + 1];
				currNode.key[i + 1] = currNode.key[i];
			}
			currNode.key[currPath] = medianKey;
			currNode.ptr[currPath + 1] = newIndex;
			currNode.keyCnt++;
			this.buffer.write(currNode.index, currNode);
			medianKey = nextMedianKey;
			newIndex = nextIndex;
		}
		
		if(newIndex >= 0)//split the root
		{
			newNode = new BPlusTreeNode(this.root.keyType);
			newNode.index = this.buffer.allocateEntry();
			newNode.key[0] = medianKey;
			newNode.ptr[0] = this.root.index;
			newNode.ptr[1] = newIndex;
			newNode.keyCnt = 2;
			this.root = newNode;
			this.buffer.write(newNode.index, newNode);
			this.height++;
		}
		return true;
	}
	
	public boolean deleteRecord(SQLElement key)
	{
		if(!this.find(key))
			return false;
		this.stackPtr--;
		BPlusTreeNode currNode = this.nodeStack[this.stackPtr];
		int path = this.pathStack[this.stackPtr];
		for(int i = path; i < 2; i++)
		{
			currNode.key[i] = currNode.key[i + 1];
			currNode.ptr[i] = currNode.ptr[i + 1];
		}
		currNode.key[2] = null;
		currNode.ptr[2] = -1;
		if(currNode.keyCnt == 4)
			currNode.keyCnt = 2;
		else
			currNode.keyCnt--;
		if(currNode.keyCnt >= 1)
		{
			this.buffer.write(currNode.index, currNode);
			return true;
		}
		if(this.stackPtr == 0)
		{
			this.buffer.removeEntry(currNode.index);
			this.root.index = -1;
			this.height = -1;
			return true;
		}
		
		if(currNode.ptr[3] >= 0)
		{
			BPlusTreeNode nextNode = this.buffer.read(currNode.ptr[3], this.root);
			nextNode.prev = currNode.prev;
			this.buffer.write(nextNode.index, nextNode);
		}
		if(currNode.prev >= 0)
		{
			BPlusTreeNode prevNode = this.buffer.read(currNode.prev, this.root);
			prevNode.ptr[3] = currNode.ptr[3];
			this.buffer.write(prevNode.index, prevNode);
		}
		this.buffer.removeEntry(currNode.index);
		
		this.stackPtr--;
		currNode = this.nodeStack[this.stackPtr];
		path = this.pathStack[this.stackPtr];
		shiftContent(currNode, path);
		//Cascade merge
		while(currNode.keyCnt == 1 && this.stackPtr > 0)
		{
			this.stackPtr--;
			BPlusTreeNode parent = this.nodeStack[this.stackPtr];
			path = this.pathStack[this.stackPtr];
			if(path == 0)
			{
				BPlusTreeNode sibling = this.buffer.read(parent.ptr[1], this.root);
				if(sibling.keyCnt > 2)
				{
					currNode.keyCnt = 2;
					currNode.ptr[1] = sibling.ptr[0];
					currNode.key[0] = parent.key[0];
					parent.key[0] = sibling.key[0];
					shiftContent(sibling, 0);
				}
				else
				{
					sibling.ptr[2] = sibling.ptr[1];
					sibling.ptr[1] = sibling.ptr[0];
					sibling.ptr[0] = currNode.ptr[0];
					sibling.key[1] = sibling.key[0];
					sibling.key[0] = parent.key[0];
					sibling.keyCnt++;
					shiftContent(parent, 0);
					this.buffer.removeEntry(currNode.index);
				}
			}
			else
			{
				BPlusTreeNode sibling = this.buffer.read(parent.ptr[path - 1], this.root);
				if(sibling.keyCnt > 2)
				{
					sibling.keyCnt--;
					currNode.keyCnt = 2;
					currNode.ptr[1] = currNode.ptr[0];
					currNode.key[0] = parent.key[path - 1];
					currNode.ptr[0] = sibling.ptr[sibling.keyCnt];
					parent.key[path - 1] = sibling.key[sibling.keyCnt - 1];
					sibling.ptr[sibling.keyCnt] = -1;
					sibling.key[sibling.keyCnt - 1] = null;
				}
				else
				{
					sibling.key[1] = parent.key[path - 1];
					sibling.ptr[2] = currNode.ptr[0];
					sibling.keyCnt++;
					shiftContent(parent, path);
					this.buffer.removeEntry(currNode.index);
				}
			}
			currNode = parent;
		}
		if(currNode.keyCnt == 1)
		{
			this.height--;
			this.root = this.buffer.read(currNode.ptr[0], this.root);
		}
		return true;
	}
	
	private static void shiftContent(BPlusTreeNode n, int index)
	{
		for(int i = index; i < 3; i++)
		{
			n.ptr[i] = n.ptr[i + 1];
			if(i > 0) n.key[i - 1] = n.key[i];
		}
		n.keyCnt--;
		n.key[2] = null;
		n.ptr[3] = -1;
	}
	
	protected boolean find(SQLElement value)
	{
		this.stackPtr = 0;
		if(this.height < 0)
			return false;
		int depth, i;
		BPlusTreeNode currNode = this.root;
		for(depth = 0; depth < this.height; depth++)
		{
			for(i = 0; i < 3; i++)
				if(i > currNode.keyCnt - 2 || currNode.key[i].compareTo(value) > 0) break;
			this.nodeStack[this.stackPtr] = currNode;
			this.pathStack[this.stackPtr] = i;
			this.stackPtr++;
			currNode = this.buffer.read(currNode.ptr[i], this.root);
		}
		this.nodeStack[this.stackPtr] = currNode;
		int cmp;
		boolean found = false;
		for(i = 0; i < 3; i++)
		{
			if(i >= currNode.keyCnt)
				break;
			cmp = currNode.key[i].compareTo(value);
			if(cmp >= 0)
			{
				found = (cmp == 0);
				break;
			}
		}
		this.pathStack[this.stackPtr] = i;
		this.stackPtr++;
		return found;
	}
	
	public static class BPlusTreeNode implements SQLSerializable<BPlusTreeNode>
	{
		protected SQLElement keyType;
		public SQLElement[] key = {null, null, null};
		public int[] ptr = {-1, -1, -1, -1};
		public int prev = -1;//Used only in leaf node
		public int index = -1;
		protected byte keyCnt = -1;
		
		public BPlusTreeNode(SQLElement keyType)
		{
			this.keyType = keyType;
		}
		
		@Override
		public int getSize()
		{
			return 20 + 3 * this.keyType.getSize();
		}
	
		@Override
		public void write(byte[] block, int offset)
		{
			writeInt(block, offset, this.ptr[0]);
			writeInt(block, offset + 4, this.ptr[1]);
			writeInt(block, offset + 8, this.ptr[2]);
			writeInt(block, offset + 12, this.ptr[3]);
			writeInt(block, offset + 16, this.prev);
			offset += 20;
			if(this.key[0] != null)
				this.key[0].write(block, offset);
			offset += this.keyType.getSize();
			if(this.key[1] != null)
				this.key[1].write(block, offset);
			offset += this.keyType.getSize();
			if(this.key[2] != null)
				this.key[2].write(block, offset);
		}
	
		@Override
		public BPlusTreeNode read(byte[] block, int offset)
		{
			BPlusTreeNode ret = new BPlusTreeNode(this.keyType);
			ret.ptr[0] = readInt(block, offset);
			ret.ptr[1] = readInt(block, offset + 4);
			ret.ptr[2] = readInt(block, offset + 8);
			ret.ptr[3] = readInt(block, offset + 12);
			ret.prev = readInt(block, offset + 16);
			offset += 20;
			ret.key[0] = this.keyType.read(block, offset);
			offset += this.keyType.getSize();
			ret.key[1] = this.keyType.read(block, offset);
			offset += this.keyType.getSize();
			ret.key[2] = this.keyType.read(block, offset);
			if(ret.ptr[0] == -1)
				ret.keyCnt = 0;
			else if(ret.ptr[1] == -1)
				ret.keyCnt = 1;
			else if(ret.ptr[2] == -1)
				ret.keyCnt = 2;
			else if(ret.ptr[3] == -1)
				ret.keyCnt = 3;
			else
				ret.keyCnt = 4;
			return ret;
		}

		private static void writeInt(byte[] block, int offset, int value)
		{
			block[offset++] = (byte)value;
			block[offset++] = (byte)(value >> 8);
			block[offset++] = (byte)(value >> 16);
			block[offset] = (byte)(value >> 24);
		}
		
		private static int readInt(byte[] block, int offset)
		{
			int i;
			i = block[offset + 3] & 255;
			i = (i << 8) | (block[offset + 2] & 255);
			i = (i << 8) | (block[offset + 1] & 255);
			i = (i << 8) | (block[offset] & 255);
			return i;
		}
		
	}

	public class IntIterator
	{
		private BPlusTreeNode currNode;
		private int currIndex;
		
		protected IntIterator(BPlusTreeNode initNode, int initIndex)
		{
			this.currNode = initNode;
			this.currIndex = initIndex;
		}
		
		public int next()
		{
			int ret = this.currNode.ptr[this.currIndex];
			if(ret < 0) return ret;
			this.currIndex++;
			if(this.currIndex == 3 || this.currNode.ptr[this.currIndex] < 0)
			{
				if(this.currNode.ptr[3] >= 0)
				{
					this.currNode = buffer.read(this.currNode.ptr[3], root);
					this.currIndex = 0;
				}
			}
			return ret;
		}

		public int prev()
		{
			int ret = this.currNode.ptr[this.currIndex];
			if(ret < 0) return ret;
			this.currIndex--;
			if(this.currIndex < 0 || this.currNode.ptr[this.currIndex] < 0)
			{
				if(this.currNode.prev >= 0)
				{
					this.currNode = buffer.read(this.currNode.prev, root);
					this.currIndex = 2;
				}
			}
			return ret;
		}
		
		public IntIterator clone()
		{
			return new IntIterator(this.currNode, this.currIndex);
		}
	}
}
