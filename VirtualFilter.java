import java.util.BitSet;
import java.util.Random;

public class VirtualFilter {
	
	public long totalLength = 0;
	public long realPartLength = 0;
	public double samplingRate = 0.0;
	public long randomSeeds = 0L;
	public long zeros = 0L;
	public BitSet filter;

	VirtualFilter(long totalLength, long realPartLength, double samplingRate) {
		this.totalLength = totalLength;
		this.realPartLength = realPartLength;
		this.samplingRate = samplingRate;
		filter = new BitSet((int)realPartLength);
		zeros = realPartLength;
		randomSeeds = new Random().nextLong();
	}
	
	//check whether virtual filter is still workable
	public boolean isWorkable() {
		return (double)zeros > (double)totalLength * samplingRate;
	}
	
	//Tomas Wang Hash
	public int FNVHash(long key) {
		  key = (~key) + (key << 18); 
		  key = key ^ (key >>> 31);
		  key = key * 21;
		  key = key ^ (key >>> 11);
		  key = key + (key << 6);
		  key = key ^ (key >>> 22);
		  return (int) key;
	}
	
	//filter an item
	public boolean filter(long item) {
		long i = ((long)FNVHash(FNVHash(item ^ randomSeeds)) % totalLength + totalLength) % totalLength;
		//Step 1
		if (i < realPartLength) {
			//Step 2
			if (!filter.get((int)i)) {
				filter.set((int)i);
				//Step 3
				if ((double)i / (double)realPartLength < (double)totalLength * samplingRate / (double)zeros && isWorkable()) {
					zeros--;
					return true;
				}
				zeros--;
			}
		}
		return false;
	}
}
