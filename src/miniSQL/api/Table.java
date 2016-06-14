package miniSQL.api;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;

import miniSQL.buffer.FileBuffer;
import miniSQL.buffer.SubBuffer;

public class Table implements Iterable<Record>
{
	private ArrayList<Column> schema = new ArrayList<>();
	private FileBuffer buffer;
	private SubBuffer<Record> recordBuffer;
	
	private static final Predicate<Record> predicatePlaceholder = r -> true;
	private BPlusTree.IntIterator currIterator = null;
	private boolean iterateDirection = false;//true for forward, false for backward
	private Predicate<Record> currPredicate = predicatePlaceholder;
	
	private Record dummyRecord = new Record(this);
	private Record uniqueRecord = null;
	
	public Table(FileBuffer buffer)
	{
		this.buffer = buffer;
		this.recordBuffer = buffer.getSubBuffer(1);
		SubBuffer<Column> columnBuffer = buffer.getSubBuffer(0);
		Column dummy = new Column(this);
		int j = columnBuffer.getMaxBlockIndex();
		for(int i = 0; i < j; i++)
		{
			if(!columnBuffer.isBlockValid(i))
				continue;
			this.schema.add(columnBuffer.read(i, dummy));
		}
	}
	
	public Table(FileBuffer buffer, List<Column> schema)
	{
		this.schema.addAll(schema);
		this.buffer = buffer;
		buffer.createSubBuffer(schema.get(0).getSize());
		SubBuffer<Column> columnBuffer = buffer.getSubBuffer(0);
		int recordSize = 0;
		for(Column e : schema)
		{
			recordSize += e.getType().getSize();
			columnBuffer.write(columnBuffer.allocateBlock(), e);
			e.setOwner(this);
			if(e.isPrimary())
				e.createIndex(false);
		}
		buffer.createSubBuffer(recordSize);
		this.recordBuffer = buffer.getSubBuffer(1);
	}
	
	public void selectAnd(Column column, SQLPredicate predicate, SQLElement value)
	{
		this.selectAnd(this.schema.indexOf(column), predicate, value);
	}
	
	public void selectAnd(int columnIndex, SQLPredicate predicate, SQLElement value)
	{
		Column column = this.schema.get(columnIndex);
		boolean b = column.isIndexed() && this.uniqueRecord == null;
		if(b && predicate == SQLPredicate.EQUAL)
		{
			int recIndex = column.getIndex().findRecord(value);
			if(recIndex >= 0)
				this.uniqueRecord = this.recordBuffer.read(recIndex, this.dummyRecord);
			else
				this.uniqueRecord = this.dummyRecord;//Mark as no selection
		}
		else if(b && this.currIterator == null && predicate != SQLPredicate.NOTEQUAL)
		{
			boolean found = column.getIndex().find(value);
			this.currIterator = column.getIndex().getIterator();
			switch(predicate) {
			case GREATER:
				if(found) this.currIterator.next();
				break;
			case LESS:
				this.currIterator.prev();
				break;
			case LEQUAL:
				if(!found) this.currIterator.prev();
				break;
			default:
				break;
			}
			this.iterateDirection = predicate == SQLPredicate.GREATER || predicate == SQLPredicate.GEQUAL;
		}
		else
		{
			Predicate<Record> p = null;
			switch(predicate) {
			case EQUAL:
				p = r -> r.get(columnIndex).compareTo(value) == 0;
				break;
			case NOTEQUAL:
				p = r -> r.get(columnIndex).compareTo(value) != 0;
				break;
			case GEQUAL:
				p = r -> r.get(columnIndex).compareTo(value) >= 0;
				break;
			case LEQUAL:
				p = r -> r.get(columnIndex).compareTo(value) <= 0;
				break;
			case GREATER:
				p = r -> r.get(columnIndex).compareTo(value) > 0;
				break;
			case LESS:
				p = r -> r.get(columnIndex).compareTo(value) < 0;
				break;
			}
			if(this.currPredicate == predicatePlaceholder)
				this.currPredicate = p;
			else
				this.currPredicate = this.currPredicate.and(p);
		}
	}
	
	public void clearSelect()
	{
		this.currIterator = null;
		this.currPredicate = predicatePlaceholder;
	}
	
	public boolean insert(Record record)
	{
		this.clearSelect();
		for(Record rec : this)
		{
			for(int i = 0; i < this.schema.size(); i++)
				if(this.schema.get(i).isUnique() && rec.get(i).compareTo(record.get(i)) == 0)
					return false;
		}
		int i = this.recordBuffer.allocateBlock();
		this.recordBuffer.write(i, record);
		return true;
	}
	
	public List<Column> getColumns()
	{
		return this.schema;
	}
	
	public int getColumn(String columnName)
	{
		for(int i = 0; i < this.schema.size(); i++)
			if(this.schema.get(i).getName().equals(columnName))
				return i;
		return -1;
	}

	@Override
	public Iterator<Record> iterator()
	{
		if(this.uniqueRecord != null)
		{
			if(this.uniqueRecord == this.dummyRecord)
				return new UniqueIterator(null);
			else if(this.currPredicate.test(this.uniqueRecord))
				return new UniqueIterator(this.uniqueRecord);
			else
				return new UniqueIterator(null);
		}
		else if(this.currIterator != null)
			return new IndexedIterator(this.currIterator, this.iterateDirection, this.currPredicate);
		else
			return new LinearIterator(this.currPredicate);
	}
	
	protected FileBuffer getBuffer()
	{
		return this.buffer;
	}
	
	protected SubBuffer<Record> getRecBuffer()
	{
		return this.recordBuffer;
	}

	protected class LinearIterator implements Iterator<Record>
	{
		private int currIndex;
		private int maxIndex;
		private Record nextRecord;
		private Predicate<Record> currPredicate;
		
		public LinearIterator(Predicate<Record> predicate)
		{
			this.currIndex = 0;
			this.maxIndex = recordBuffer.getMaxBlockIndex();
			this.nextRecord = null;
			this.currPredicate = predicate;
		}
		
		@Override
		public boolean hasNext()
		{
			do
			{
				while(!recordBuffer.isBlockValid(currIndex) && currIndex < maxIndex)
					currIndex++;
				if(currIndex >= maxIndex)
					return false;
				else
				{
					this.nextRecord = recordBuffer.read(currIndex, dummyRecord);
					currIndex++;
				}
			}
			while(!currPredicate.test(nextRecord));
			return true;
		}

		@Override
		public Record next()
		{
			if(this.nextRecord != null)
			{
				Record r = this.nextRecord;
				this.nextRecord = null;
				return r;
			}
			else if(this.hasNext())
			{
				Record r = this.nextRecord;
				this.nextRecord = null;
				return r;
			}
			else
				return null;
		}
		
	}
	
	protected class IndexedIterator implements Iterator<Record>
	{
		private BPlusTree.IntIterator it;
		private boolean direction;
		private Predicate<Record> predicate;
		private Record nextRecord = null;
		
		public IndexedIterator(BPlusTree.IntIterator iterator, boolean direction, Predicate<Record> predicate)
		{
			this.it = iterator.clone();
			this.direction = direction;
			this.predicate = predicate;
		}
		
		@Override
		public boolean hasNext()
		{
			do
			{
				int i;
				if(this.direction)
					i = this.it.next();
				else
					i = this.it.prev();
				if(i == -1)
					return false;
				this.nextRecord = recordBuffer.read(i, dummyRecord);
			}
			while(!this.predicate.test(this.nextRecord));
			return true;
		}

		@Override
		public Record next()
		{
			if(this.nextRecord != null)
			{
				Record r = this.nextRecord;
				this.nextRecord = null;
				return r;
			}
			else if(this.hasNext())
			{
				Record r = this.nextRecord;
				this.nextRecord = null;
				return r;
			}
			else
				return null;
		}
		
	}
	
	protected class UniqueIterator implements Iterator<Record>
	{
		private Record rec;
		
		public UniqueIterator(Record rec)
		{
			this.rec = rec;
		}
		
		@Override
		public boolean hasNext()
		{
			return rec == null;
		}

		@Override
		public Record next()
		{
			Record r = this.rec;
			this.rec = null;
			return r;
		}
		
	}
}
