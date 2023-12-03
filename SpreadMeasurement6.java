import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;
import java.util.Scanner;
//in hereti sprea measuremtn 5 but there the artifitial flows are from the original dataset so we need to remove them first
public class SpreadMeasurement6 {
	
	public static long totalLength = 500000L;
	public static long realPartLength = 150000L;
	public static long M = 100000L;
	public static int fileNum =1;
	public static double p = 0.1;
	//dataStream location, every line is srcAddress dstAddress
	//We use dstAddress as flow label
	//srcAddress dstAddress are transformed to int type
	//We use hashMap to store spreads, other options will be CountMin, CountSketch...
	public static HashMap<Long, Double> spreads = new HashMap<>();
	public static VirtualFilter VF;
	
	public static double epsilon = 0.1;
	public static double beta = 100;
	public static int k = 0;
	public static int t = 0;
	public static int encodeLargeNum = 0;
	public static long[] flowida = new long[60000000];
	public static long[] elementida = new long[60000000];
	public static int[] pos  = new int[60000000];
	public static int pkt = 0;
	
	public static long[] Marray ={12800000L};//12800000L, 25600000L, 51200000L, 102400000L};//{100000L, 200000L, 400000L, 800000L, 1600000L, 3200000L, 6400000L, 12800000L};
	public static double[] Barray = {100};   //{32};//{2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096};// value array of beta in the paper
	public static double[] Earray = {0.1};   //{0.01, 0.02, 0.05, 0.1, 0.2}; //value array of epsilon in the paper
	
	public static long[][][] result = new long[Marray.length][Barray.length][Earray.length];
	public static long[][][] result1 = new long[Marray.length][Barray.length][Earray.length];

	
	public static HashMap<Long, HashSet<Long>> elements = new HashMap<>();
	public static int base = 100;
	
	public static long[] rA = new long[2*base];
        
    public static int[] largeFlow = {72081,71638,48159,45912,45410,22873,15523,14306,14076,13965};
    public static boolean frontLoad = false;    
    public static long[] largeFlowLabel = {853564128L,853564135L,1672847798L,1672847805L,1672847806L,
        		1641058796L,
        		949241457L,
        		853564130L,
        		2303198213L,
        		853564137L};
        
    public static int largeFlowTot = 0;
    
    public static int accuracyNum =  20000000;
    public static boolean estimateAccuracy = true;
	public static HashMap<Long, Double> realSpread = new HashMap<>(), sumSpread= new HashMap<>(),stderr = new HashMap<>();
	public static HashMap<Long, Long> flowid = new HashMap<>(), elementid= new HashMap<>();
	public static boolean fullRandomID = true;
	public static int repeatTime = 1000;
	public static double getTK(int i) {
		if (i <= k) {
			return (1.0 + epsilon * epsilon * beta) * (double)i;
		} else {
			double x = Math.pow((1.0 + epsilon *epsilon)/(1.0 - epsilon * epsilon), i - k);
			return x * getTK(k) + (x - 1.0) / 2.0 / epsilon / epsilon;
		}
	}
	
	public static double getP(int i) {
		if ((double)i < beta * p) {
			return p;
		} else {
			return Math.max(0.0, (1.0 - epsilon * epsilon)/(2.0 * epsilon * epsilon * getTK(i) + 1.0));
		}
	}
	public static long[] encodeSpread(String filePath) throws FileNotFoundException {
		System.out.println("Encoding elements!" + "M :" + M + ", beta :" + beta + ", epsilon :" + epsilon);
		//Scanner sc = new Scanner(new File(filePath));
		Random ran = new Random();
		spreads.clear();
		long seed = ran.nextLong();
		int i = 0;
		int j = 0;
		
		encodeLargeNum =0;
	if(frontLoad) {
        for (long fff : largeFlowLabel) {
            for (long e : elements.get(fff)) {
            	int base1 = 1;
            	for (j = 0; j < base1 && VF.isWorkable(); j++) {
            		double p1 = getP(spreads.getOrDefault(fff, 0.0).intValue());
            		long e1 = e ^ rA[j];
            		
            		int hash = GeneralUtil.FNVHash1(seed ^ (( fff << 32) | e1));
            		if (((double)hash - (double)Integer.MIN_VALUE) / ((double)Integer.MAX_VALUE +1.0) / 2.0 < p1 / p && VF.filter(( fff << 32) | e1)) {
            			spreads.put(fff, spreads.getOrDefault(fff, 0.0) + 1);
            		}
            		if (!VF.isWorkable()) {
						break;
            		}
            		encodeLargeNum +=1;
            		if (estimateAccuracy && encodeLargeNum == accuracyNum)
            			copyEstimate();
            	}
            	if (!VF.isWorkable()) {
            			break;
            	}
            }
            if (!VF.isWorkable()) {
				break;
            }
        }
	}
	long startTime = System.nanoTime();	
	int encodeBaseNum = 0;
		while (i < pkt && VF.isWorkable()) {
			int base1 = 1;
			for (j = 0; j < base1 && VF.isWorkable(); j++) {
				encodeBaseNum +=1;
				long e = elementida[i] ^ rA[j];
				
				double p1 = getP(spreads.getOrDefault(flowida[i], 0.0).intValue());
				int hash = (GeneralUtil.FNVHash1(seed ^ ((flowida[i] << 32) | e))%10000000+10000000)%10000000;
				if (hash < p1 / p *10000000&& VF.filter((flowida[i] << 32) | e)) {
					spreads.put(flowida[i], spreads.getOrDefault(flowida[i], 0.0) + 1);
				}
			}
			if (!VF.isWorkable()) {
				System.out.println("---------"+encodeBaseNum);break;
				
			}
			i++;
			if (estimateAccuracy && encodeBaseNum+encodeLargeNum == accuracyNum)
				{copyEstimate(); }
			if (i % 1000000 == 0) {
				//System.out.println(i);
			}
		}
		long endTime = System.nanoTime();
		double duration = 1.0 * (endTime - startTime) ;
		System.out.println("processing time ns : "+duration/pkt);
		//System.out.println("totalencode="+i);
		//sc.close();
		return new long[] {i == pkt ? (long)pkt * (long)base+ encodeLargeNum : (long)i * (long)base + (long)j+ encodeLargeNum, i == pkt ? (long)t * (long)base + encodeLargeNum: (long)pos[i] * (long)base + (long)j+ encodeLargeNum};
	}
	public static void copyEstimate() {
		
		for (long fff: spreads.keySet()) {
			sumSpread.put(fff, sumSpread.getOrDefault(fff, 0.0) + spreads.getOrDefault(fff,0.0));
			stderr.put(fff,stderr.getOrDefault(fff, 0.0)+ Math.pow( getTK(spreads.getOrDefault(fff,0.0).intValue())-realSpread.getOrDefault(fff,0.0),2));
			
		}
		
	}
	public static void querySpread() throws FileNotFoundException {
		for (long fff: sumSpread.keySet()) {
			sumSpread.put(fff,sumSpread.getOrDefault(fff, 0.0)/repeatTime);
			stderr.put(fff,Math.sqrt(stderr.getOrDefault(fff, 0.0)/repeatTime));
			
		}
		System.out.println("Query Spreads!" );
		//Scanner sc = new Scanner(new File(filePath1));
		System.out.println("query spread");
		String resultFilePath = GeneralUtil.path+"EBSampling\\AccuracyEBSampling_spreadnumber_"+accuracyNum+"largeFLowSPread"+largeFlowTot+"_e_"+epsilon+"_b_"+beta+"_M_"+M+"_base_"+base;
		if (frontLoad)resultFilePath = GeneralUtil.path+"EBSampling\\AccuracyFrontLoadEBSampling_spreadnumber_"+accuracyNum+"largeFLowSPread"+largeFlowTot+"_e_"+epsilon+"_b_"+beta+"_M_"+M+"_base_"+base;
		String resultFilePath1 = GeneralUtil.path+"EBSampling\\AccuracyEBSampling_spreadnumber_"+accuracyNum+"largeFLowSPread"+largeFlowTot+"_e_"+epsilon+"_b_"+beta+"_M_"+M+"_base_"+base+"STANDARERROR";
		if (frontLoad)resultFilePath1 = GeneralUtil.path+"EBSampling\\AccuracyFrontLoadEBSampling_spreadnumber_"+accuracyNum+"largeFLowSPread"+largeFlowTot+"_e_"+epsilon+"_b_"+beta+"_M_"+M+"_base_"+base+"STANDARERROR";
		String resultFilePath2 = GeneralUtil.path+"EBSampling\\AccuracyEBSampling_spreadnumber_"+accuracyNum+"largeFLowSPread"+largeFlowTot+"_e_"+epsilon+"_b_"+beta+"_M_"+M+"_base_"+base+"RelativeSTANDARERROR";
		if (frontLoad)resultFilePath2 = GeneralUtil.path+"EBSampling\\AccuracyFrontLoadEBSampling_spreadnumber_"+accuracyNum+"largeFLowSPread"+largeFlowTot+"_e_"+epsilon+"_b_"+beta+"_M_"+M+"_base_"+base+"RelativeSTANDARERROR";
		
		PrintWriter pw = new PrintWriter(new File(resultFilePath));
		PrintWriter pw1 = new PrintWriter(new File(resultFilePath1));
		PrintWriter pw2 = new PrintWriter(new File(resultFilePath2));

		for (long fff:sumSpread.keySet()) {	
			int spread = (int)getTK(sumSpread.getOrDefault(fff, 0.0).intValue());
			pw.println(fff + "\t" +realSpread.getOrDefault(fff,0.0).intValue()+"\t"+ spread);
			if (realSpread.getOrDefault(fff,0.0).intValue()>= beta)
				pw1.println(fff + "\t" +realSpread.getOrDefault(fff,0.0).intValue()+"\t"+ (realSpread.getOrDefault(fff,0.0).intValue()+stderr.getOrDefault(fff,0.0) ));
			if (realSpread.getOrDefault(fff,0.0).intValue()>= beta)
				pw2.println(fff + "\t" +realSpread.getOrDefault(fff,0.0).intValue()+"\t"+ stderr.getOrDefault(fff,0.0)/realSpread.getOrDefault(fff,0.0) );
		}

		
		pw.close();
		pw1.close();
		pw2.close();
		GeneralUtil.analyzeAccuracy(resultFilePath);
		GeneralUtil.analyzeAccuracy(resultFilePath1);
		
	}
	
	public static void init() {
		p = 1.0 / (1.0 + epsilon * epsilon * beta);
		k = (int)Math.floor(beta / (1.0 + epsilon * epsilon * beta));
		realPartLength = M;
		if (p < 1.0 / Math.E) {
			totalLength = (long)((double)M / p / Math.E); 
		} else {
			totalLength = M;
		}
		Random ran = new Random();
		HashSet<Long> s = new HashSet<>();
		for (int i = 0; i < 2*base; i++) {
			rA[i] = ran.nextLong() & 0xFFFFFFFF;
			while (s.contains(rA[i])) {
				rA[i] = ran.nextLong() & 0xFFFFFFFF;
			}
			s.add(rA[i]);
		}
                
	}
	public static void loadStream() throws FileNotFoundException {
		
		elements = new HashMap<>();
		for (int i =0; i< fileNum; i++) {
		Scanner sc = new Scanner(new File(GeneralUtil.dataStreamForFlowSpread+i+"v.txt"));
        Random ran1 = new Random();

		t = 0;
		while (sc.hasNextLine()){
			String entry = sc.nextLine();
			entry = entry.replace(".", "");
			String[] strs = entry.split("\\s+");
			String[] res = GeneralUtil.getSperadFlowIDAndElementID(strs, true);
			flowida[pkt] = Long.parseLong(res[0]);
			int j =0;
			for ( j =0 ; j< largeFlowLabel.length; j++) {
				if (frontLoad &&flowida[pkt]==largeFlowLabel[j])
					break;
			}
			if (frontLoad &&j < largeFlowLabel.length) continue;
			int base1 = ran1.nextInt(2*base);
			elementida[pkt] = Long.parseLong(res[1]);
			if (fullRandomID) {
				if (flowid.containsKey( flowida[pkt])) {
					flowida[pkt] = flowid.get(flowida[pkt]);
					for (int jjj =1; jjj< base1; jjj++) {
						flowida[pkt+jjj] = flowida[pkt];
					}
				}
				else {
					long randFlowid =  ran1.nextLong();
					while (flowid.containsKey(randFlowid)) {
						randFlowid =  ran1.nextLong();
					}
					flowid.put(flowida[pkt], randFlowid);
					
					flowida[pkt] = randFlowid;
					for (int jjj =1; jjj< base1; jjj++) {
						flowida[pkt+jjj] = flowida[pkt];
					}
				}
				if (elementid.containsKey( elementida[pkt])) {
					elementida[pkt] = elementid.get(elementida[pkt]);
					for (int jjj =1; jjj< base1; jjj++) {
						long randElemid1 =  ran1.nextLong();
						while (elementid.containsKey(randElemid1)) {
							randElemid1 =  ran1.nextLong();
						}
						elementid.put(elementida[pkt], randElemid1);
						elementida[pkt+jjj] = randElemid1;
					}
				}
				else {
					long randElemid =  ran1.nextLong();
					while (elementid.containsKey(randElemid)) {
						randElemid =  ran1.nextLong();
					}
					elementid.put(elementida[pkt], randElemid);
					elementida[pkt] = randElemid;
					for (int jjj =1; jjj< base1; jjj++) {
						long randElemid1 =  ran1.nextLong();
						while (elementid.containsKey(randElemid1)) {
							randElemid1 =  ran1.nextLong();
						}
						elementid.put(elementida[pkt], randElemid1);
						elementida[pkt+jjj] = randElemid1;
					}
				}
				
				
			}
			elements.putIfAbsent(flowida[pkt], new HashSet<>());
			if (!elements.get(flowida[pkt]).contains(elementida[pkt])) {
				for (int jjj = 0; jjj<base1; jjj++) {
				elements.get(flowida[pkt]).add(elementida[pkt]);
				if (largeFlowTot+pkt <= accuracyNum) realSpread.put(flowida[pkt],  realSpread.getOrDefault(flowida[pkt], 0.0)+1);
				pos[pkt] = t;
				pkt++;
				if (!(largeFlowTot+pkt <= accuracyNum)) break;
				if (pkt % 100000 == 0) {
					System.out.println(pkt);
				}
				}
				if (!(largeFlowTot+pkt <= accuracyNum)) break;
			}
			t++;
		}
		
		sc.close();
		}
		
	}
	
	public static void printResult() throws FileNotFoundException {
		
		//per Memory
		for (int i = 0; i < Marray.length; i++) {
			String resultFilePath = GeneralUtil.path + "EBSampling\\EBSamplingLargeFlowFrontLoad_"+ largeFlowTot + "_M_" + Marray[i]+"_base_"+base;
			PrintWriter pw = new PrintWriter(new File(resultFilePath));
			pw.print("0");
			for (int k = 0; k < Earray.length; k++) {
				pw.print("\t" + Earray[k]);
			}
			pw.println();
			for (int j = 0; j < Barray.length; j++) {
				pw.print(Barray[j]);
				for (int k = 0; k < Earray.length; k++) {
					pw.print("\t" + (result[i][j][k] ));
				}
				pw.println();
			}
			pw.close();
		}
		
		//per beta
		for (int j = 0; j < Barray.length; j++) {
			String resultFilePath = GeneralUtil.path + "EBSampling\\EBSamplingLargeFlowFrontLoad_"+ largeFlowTot+"_beta_" + Barray[j]+"_base_"+base;
			PrintWriter pw = new PrintWriter(new File(resultFilePath));
			pw.print("0");
			for (int k = 0; k < Earray.length; k++) {
				pw.print("\t" + Earray[k]);
			}
			pw.println();
			for (int i = 0; i < Marray.length; i++) {
				pw.print(Marray[i]);
				for (int k = 0; k < Earray.length; k++) {
					pw.print("\t" + (result[i][j][k] ));
				}
				pw.println();
			}
			pw.close();
		}
		
		//per epsilon
		for (int k = 0; k < Earray.length; k++) {
			String resultFilePath = GeneralUtil.path + "EBSampling\\EBSamplingLargeFlowFrontLoad_"+ largeFlowTot+"_epsilon_" + Earray[k]+"_base_"+base;
			PrintWriter pw = new PrintWriter(new File(resultFilePath));
			pw.print("0");
			for (int j = 0; j < Barray.length; j++) {
				pw.print("\t" + Barray[j]);
			}
			pw.println();
			for (int i = 0; i < Marray.length; i++) {
				pw.print(Marray[i]);
				for (int j = 0; j < Barray.length; j++) {
					pw.print("\t" + (result[i][j][k] ));
				}
				pw.println();
			}
			pw.close();
		}
	}
	
	public static void printResult1() throws FileNotFoundException {
		
		//per Memory
		for (int i = 0; i < Marray.length; i++) {
			String resultFilePath = GeneralUtil.path + "EBSampling\\EBSamplingLargeFlowFrontLoad_"+ largeFlowTot + "_M_" + Marray[i]+"_base_"+base+"P";
			PrintWriter pw = new PrintWriter(new File(resultFilePath));
			pw.print("0");
			for (int k = 0; k < Earray.length; k++) {
				pw.print("\t" + Earray[k]);
			}
			pw.println();
			for (int j = 0; j < Barray.length; j++) {
				pw.print(Barray[j]);
				for (int k = 0; k < Earray.length; k++) {
					pw.print("\t" + (result1[i][j][k]));
				}
				pw.println();
			}
			pw.close();
		}
		
		//per beta
		for (int j = 0; j < Barray.length; j++) {
			String resultFilePath = GeneralUtil.path + "EBSampling\\EBSamplingLargeFlowFrontLoad_"+ largeFlowTot+"_beta_" + Barray[j]+"_base_"+base+"P";
			PrintWriter pw = new PrintWriter(new File(resultFilePath));
			pw.print("0");
			for (int k = 0; k < Earray.length; k++) {
				pw.print("\t" + Earray[k]);
			}
			pw.println();
			for (int i = 0; i < Marray.length; i++) {
				pw.print(Marray[i]);
				for (int k = 0; k < Earray.length; k++) {
					pw.print("\t" + (result1[i][j][k]));
				}
				pw.println();
			}
			pw.close();
		}
		
		//per epsilon
		for (int k = 0; k < Earray.length; k++) {
			String resultFilePath = GeneralUtil.path + "EBSampling\\EBSamplingLargeFlowFrontLoad_"+ largeFlowTot+"_epsilon_" + Earray[k]+"_base_"+base+"P";
			PrintWriter pw = new PrintWriter(new File(resultFilePath));
			pw.print("0");
			for (int j = 0; j < Barray.length; j++) {
				pw.print("\t" + Barray[j]);
			}
			pw.println();
			for (int i = 0; i < Marray.length; i++) {
				pw.print(Marray[i]);
				for (int j = 0; j < Barray.length; j++) {
					pw.print("\t" + (result1[i][j][k]));
				}
				pw.println();
			}
			pw.close();
		}
	}
	
	
	public static void main(String[] args) throws FileNotFoundException {
		for (int l : largeFlow) {
                        largeFlowTot += l;
			}
		if (!frontLoad) largeFlowTot = 0;
		loadStream();
               // largeFlowLabel = new long[largeFlow.length];
		if (frontLoad) {
			
                Random ran1 = new Random();
                for (int i = 0; i < largeFlow.length; i++) {
                    //largeFlowLabel[i] = //ran1.nextLong() & 0xFFFFFFFF;
                   // while (elements.containsKey(largeFlowLabel[i])) {
                  //     largeFlowLabel[i] = ran1.nextLong() & 0xFFFFFFFF;
                   // }
                    elements.put(largeFlowLabel[i], new HashSet<>());
                    realSpread.put(largeFlowLabel[i],largeFlow[i]*1.0);
                       while (elements.get(largeFlowLabel[i]).size() < largeFlow[i]) {
                          elements.get(largeFlowLabel[i]).add(ran1.nextLong() & 0xFFFFFFFF);
                    }
                }
		}
		for (int i = 0; i < Marray.length; i++) {
			M = Marray[i];
			for (int j = 0; j < Barray.length; j++) {
				beta = Barray[j];
				for (int k = 0; k < Earray.length; k++) {
				  for( int iii = 0; iii< repeatTime; iii++) {
					epsilon = Earray[k];
					init();
					VF = new VirtualFilter(totalLength, realPartLength, p);
					long[] tt = encodeSpread(GeneralUtil.dataStreamForFlowSpread);
					//if (estimateAccuracy) querySpread();
					result[i][j][k] = tt[0];
					result1[i][j][k] = tt[1];
				  }
				  querySpread();
				}
			}
		}
		
		printResult();
		printResult1();
		//querySpread(GeneralUtil.dataSummaryForFlowSpread);
		System.out.println("DONE!");
	}

}
