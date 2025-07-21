import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * NOTE ----> DELETE OTHNEURO COLUMN before running this code as it has commas in the description
 * TODO - 
 * 1. correct columns - 70+6
 * 2. Arrange columns
 * 3. Blanks in output
 * 4. Non number or negative in input
 */
public class PPMIDataCleanup {
	
	public static final String INPUT_FILE  = "/Users/aaryaapte/aarya/PD/PPMI_Curated_Data.csv";
	public static final String OUTPUT_FILE  = "/Users/aaryaapte/aarya/PD/PPMI_Clean_Curated_Data.csv";
	public static final String OUTPUT_FILE_1_234  = "/Users/aaryaapte/aarya/PD/PPMI_Clean_1_234_Curated_Data.csv";
	public static final String OUTPUT_FILE_ALL_plus123  = "/Users/aaryaapte/aarya/PD/PPMI_Clean_allN_plus123_Curated_Data.csv";
	public static final int MIN_VISIT_1_234 = 4;
	
	public static final String COHORT_SUBGROUP_COL = "subgroup";
	public static final String[] COHORT_SUBGROUP = {"WRONG", "GBA", "GBA + RBD", "Healthy Control", "Hyposmia", "LRRK2", "LRRK2 + GBA", "LRRK2 + VPS35",  
			 "PARK7 + RBD", "PINK1", "PRKN", "PRKN + RBD", "RBD", "SNCA", "Sporadic PD" };

	public static final String[] IN_COLUMNS  = {
			
			"PATNO", "COHORT", "subgroup", "YEAR", "age", "SEX", "race", "HISPLAT", "ASHKJEW", "AFICBERB", "BASQUE", "fampd", "handed",
			
			"APOE_e4",
			
			"PRIMDIAG", "BMI", "agediag", "ageonset", "sym_tremor", "sym_rigid", "sym_brady", "sym_posins", "sym_other", "LEDD", 
			"moca", "bjlot", "hvlt_discrimination", "hvlt_immediaterecall", "hvlt_retention", "HVLTFPRL", "HVLTRDLY", "HVLTREC",
			"lns", "SDMTOTAL", "VLTANIM", "MCI_testscores", "cogstate", "MSEADLG", 
			"quip", "quip_any", "quip_gamble", "quip_sex", "quip_buy", "quip_eat", "quip_hobby", "quip_pund", "quip_walk", "ess", "rem", "gds",
			"stai", "stai_state", "stai_trait",
			"scopa_gi", "scopa_ur", "scopa_cv", "scopa_therm", "scopa_pm", "scopa_sex", "orthostasis", 
			"pigd", "pigd_on", "td_pigd", "td_pigd_on",

			"pm_adl_any", "pm_fd_any", "pm_auto_any", "pm_cog_any", "pm_mc_any", "pm_wb_any"
	};
	
	public static final String[] OUT_COLUMNS = {
			"NHY", "NHY_ON", 
			"updrs1_score", "updrs2_score", "updrs3_score", "updrs3_score_on"
	};
	
	public static final int NHY_COL = 0;
	public static final int NHY_ON_COL = 1;
	public static final int UPDRS3_COL = 4;	
	public static final int UPDRS3_ON_COL = 5;	
	
	public static final String[] DELROW_COLUMNS = {"subgroup"};
	public static final String[] DELROW_COLUMN_VALUES = {"SWEDD"};
	
	private String inFileName;
	List<String> origCols;
	List<String[]> data;
	
	Map<Integer, Integer> occ;
	int isEmptyOnlyFirst;
	int isEmptyAllRows;
	
	public PPMIDataCleanup(String in) {
		inFileName = in;
		data = new ArrayList<String[]>();
		occ = new HashMap<Integer, Integer>();
	}
	
	public void processData () throws IOException {
		
		// input file
        FileReader f = new FileReader(inFileName);
        BufferedReader br = new BufferedReader(f);
        String cline = br.readLine();
        String[] cols = cline.split(",");
        origCols = Arrays.asList(cols);

        validateColumns();
                
        // start reading ..
        String current = br.readLine();
        int deleted = 0;
        int skipped = 0;
        int num = 0;
        
        while (current != null) {
            
            String[] tokens = current.split(",", -1);
            num++;
            if ((tokens.length == origCols.size()) &&!shouldDelete(tokens)) {
            	constructNewRow(tokens);
            }
            else {
            	if  (tokens.length != origCols.size()) {
            		System.out.println ("skipped " + " "+ num + ": " + tokens.length + " " + origCols.size());
            		skipped++;
            	}
            	else
            		deleted++;
            }
            
            current = br.readLine();
        }
        
        br.close();
        f.close();
        
        // start creating output
        printStats(deleted, skipped);
        createOutputFile(OUTPUT_FILE);
        
        newOcc_1_234();
        createOutputFile_1_234(OUTPUT_FILE_1_234, true);        
        createOutputFile_1_234(OUTPUT_FILE_ALL_plus123, false);
        printStats_1_234();
	}
	
	private void constructNewRow(String[] tokens) {
		String[] newTokens = new String[IN_COLUMNS.length  + OUT_COLUMNS.length];
		
		for (int i=0; i < IN_COLUMNS.length; i++) {
			int index= origCols.indexOf(IN_COLUMNS[i]);
			newTokens[i] = tokens[index];
		}
		int sg_index = Arrays.asList(IN_COLUMNS).indexOf(COHORT_SUBGROUP_COL);
		int sg_val_index = Arrays.asList(COHORT_SUBGROUP).indexOf(newTokens[sg_index]);
		newTokens[sg_index] = "" + sg_val_index;
		
		
		String []outTokens = new String[OUT_COLUMNS.length];
		for (int i=0; i < OUT_COLUMNS.length; i++) {
			int index= origCols.indexOf(OUT_COLUMNS[i]);
			outTokens[i] = tokens[index];
		}
		
		// massage empty output
		String nhy = outTokens[NHY_COL].trim();
		String nhy_on = outTokens[NHY_ON_COL].trim();
		String updrs3 = outTokens[UPDRS3_COL].trim();
		String updrs3_on = outTokens[UPDRS3_ON_COL].trim();
		
		if (nhy.equals("") && !nhy_on.equals("")) {
			outTokens[NHY_COL] = outTokens[NHY_ON_COL]; 
		}
		if (nhy_on.equals("") && !nhy.equals("")) {
			outTokens[NHY_ON_COL] = outTokens[NHY_COL]; 
		}
		if (updrs3.equals("") && !updrs3_on.equals("")) {
			outTokens[UPDRS3_COL] = outTokens[UPDRS3_ON_COL]; 
		}
		if (updrs3_on.equals("") && !updrs3.equals("")) {
			outTokens[UPDRS3_ON_COL] = outTokens[UPDRS3_COL]; 
		}
		
		
		// create row with output
		for (int i=0; i < OUT_COLUMNS.length; i++) {
			newTokens[IN_COLUMNS.length+i] = outTokens[i];
		}
		data.add(newTokens);
		
		// fill in occurrences
		String patStr = tokens[origCols.indexOf("PATNO")];
		int patno = Integer.parseInt(patStr);
		if (patno == 0) {
			System.out.println ("Bad PATNO " + patStr);
		}
		else {
			if (occ.get(patno) == null)
				occ.put (patno, 1);
			else {
				int occ1 = occ.get(patno);
				occ.put(patno, occ1+1);
			}
		}
	}

	private void validateColumns () {
		boolean bExit = false;
		for (String s: IN_COLUMNS) {
			if (!origCols.contains(s)) {
				bExit = true;
				System.out.println ("Column not found : " + s);
			}
		}
		for (String s: OUT_COLUMNS) {
			if (!origCols.contains(s)) {
				bExit = true;
				System.out.println ("Column not found : " + s);
			}
		}
		
		if (bExit)
			System.exit(0);
		else
			System.out.println ("Columns validated In:" + IN_COLUMNS.length + " Out:" + OUT_COLUMNS.length  + ".");
	}
	
	private boolean shouldDelete(String[] tokens) {
		for (int i=0; i < DELROW_COLUMNS.length; i++) {
			int index = origCols.indexOf(DELROW_COLUMNS[i]);
			if (tokens[index].equals(DELROW_COLUMN_VALUES[i])) {
				return true;
			}
		}
		return false;
	}
	
	private void printStats(int deleted, int skipped) {
        System.out.println ("Deleted rows: " + deleted);
        System.out.println ("Skipped rows: " + skipped);
        System.out.println();
        
		System.out.println ("--------------- Unfiltered Begin -----");

        System.out.println ("Total rows: " + (data.size()) + " Columns:" + (IN_COLUMNS.length + OUT_COLUMNS.length));
        System.out.println ();
        System.out.println ("Num patients by visits - ");

		int[] counts = new int[100];
		Iterator<Integer> iter = occ.keySet().iterator();
		while (iter.hasNext()) {
			int patno = iter.next();
			int occ1 = occ.get(patno);
			counts[occ1]++;
		}
		
		int temp = 0;
		for (int i=0; i < counts.length;i++) {
			if (counts[i] != 0) {
				System.out.println (i + ": " + counts[i]);
			}
			temp += counts[i];
		}
		
		System.out.println ("Total unique --- " + temp);
		System.out.println ("--------------- Unfiltered End -----");
	}


	
	private void createOutputFile(String outName) throws IOException {
        // output file
        FileOutputStream fo = new FileOutputStream(outName);
        PrintStream ps = new PrintStream (fo, true);
        ps.println(getArr(IN_COLUMNS) + "," + getArr(OUT_COLUMNS));

	    // write rest of the data
        int patnoIndex = indexOf(IN_COLUMNS, "PATNO");
        for (int i=0; i< data.size(); i++) {
    		String patStr = data.get(i)[patnoIndex];
    		int patno = Integer.parseInt(patStr);
    		if (occ.get(patno) != null)
    			ps.println(getArr(data.get(i)));
        }
        
        ps.flush();
        ps.close();
        fo.close();
	}
	
	private void newOcc_1_234() {
		Map<Integer, Integer> newocc = new HashMap<Integer, Integer>();
		Iterator<Integer> iter = occ.keySet().iterator();
		while (iter.hasNext()) {
			int patno = iter.next();
			int occ1 = occ.get(patno);
			if (occ1 >= MIN_VISIT_1_234)
				newocc.put(patno, occ1);
		}
		occ = newocc;
	}
	
	private void printStats_1_234() {
		System.out.println();
		System.out.println ("--------------- Next 3 year projection  Begin -----");

		System.out.println ("Minimum 4 visits - ");
		int count = 0;
		Iterator<Integer> iter = occ.keySet().iterator();
		while (iter.hasNext()) {
			int patno = iter.next();
			count  += occ.get(patno);
		}
		System.out.println ("Total rows: " + count + ", Columns: " + (IN_COLUMNS.length+4*OUT_COLUMNS.length));
		System.out.println ("Total unique: " + occ.size() + ", Columns: " + (IN_COLUMNS.length+4*OUT_COLUMNS.length));
		System.out.println();
		System.out.println ("For 1_234 - use only unique " + occ.size());
		System.out.println ("Rows with empty output: " + isEmptyOnlyFirst);
		System.out.println ("Rows in the final table: " + (occ.size() - isEmptyOnlyFirst));
		System.out.println();
		System.out.println ("For all_plus123 - " + (count - (occ.size()*3)));
		System.out.println ("Rows with empty output: " + isEmptyAllRows);
		System.out.println ("Rows in the final table: " + (count - (occ.size()*3) - isEmptyAllRows));
		System.out.println ("--------------- Next 3 year projection  End -----");

	}
	

	private void createOutputFile_1_234(String outName, boolean onlyFirst) throws IOException {
		
		// create extra out columns
		String[] newCols = new String[OUT_COLUMNS.length * 3];
		for (int i=0; i < 3; i++) {
			for  (int j=0; j < OUT_COLUMNS.length; j++) {
				newCols[OUT_COLUMNS.length*i + j] = OUT_COLUMNS[j]+"_Y" + (i+2);
			}
		}
        // output file
        FileOutputStream fo = new FileOutputStream(outName);
        PrintStream ps = new PrintStream (fo, true);
        ps.println(getArr(IN_COLUMNS) + "," + getArr(OUT_COLUMNS) + "," + getArr(newCols));

	    // write rest of the data
        int patnoIndex = indexOf(IN_COLUMNS, "PATNO");
        int prevPatNo = -1;
        
        for (int i=0; i< data.size(); i++) {
    		String patStr = data.get(i)[patnoIndex];
    		int patno = Integer.parseInt(patStr);
    		if (occ.get(patno) != null) {
    			
    			if (prevPatNo == patno)
    				continue;
    			prevPatNo = patno;
    			if (onlyFirst) {
        			boolean b = outputRow_1_234 (ps, patno, i, 0);
        			isEmptyOnlyFirst += (b)?1:0;
    			}
    			else {
    				int count = occ.get(patno);
    				count -=3;
    				for (int j=0; j < count; j++) {
    					boolean b = outputRow_1_234 (ps, patno, i+j, j);
    					isEmptyAllRows += (b)?1:0;
    				}
    			}
    		}
        }
        
        ps.flush();
        ps.close();
        fo.close();
	}
	
	private boolean outputRow_1_234(PrintStream ps, int patno, int index, int offset) {
		
		boolean isEmpty = false;
		
		String[] row = data.get(index);
		String []outVals  = new String[OUT_COLUMNS.length];
		int j=0;
		for (int i = row.length-OUT_COLUMNS.length; i < row.length; i++) {
			outVals[j++] = row[i].trim();
		}
		
		// combine multiple rows
		String[] newVals1 = new String[OUT_COLUMNS.length];
		for (int i = 0; i < OUT_COLUMNS.length; i++) {
			newVals1[i] = data.get(index+1)[data.get(index+1).length - OUT_COLUMNS.length+i].trim();
		}
		String[] newVals2 = new String[OUT_COLUMNS.length];
		for (int i = 0; i < OUT_COLUMNS.length; i++) {
			newVals2[i] = data.get(index+2)[data.get(index+2).length - OUT_COLUMNS.length+i].trim();
		}
		String[] newVals3 = new String[OUT_COLUMNS.length];
		for (int i = 0; i < OUT_COLUMNS.length; i++) {
			newVals3[i] = data.get(index+3)[data.get(index+3).length - OUT_COLUMNS.length+i].trim();
		}
		
		// massage using average
		for (int i=0; i < newVals1.length;i++) {
			if (newVals1[i].equals("")) {
				if (!outVals[i].equals("") && !newVals2[i].equals("")) {
					newVals1[i] = "" + (Integer.parseInt(outVals[i]) + Integer.parseInt(newVals2[i]))/2;
				}
			}
		}
		for (int i=0; i < newVals2.length;i++) {
			if (newVals2[i].equals("")) {
				if (!newVals1[i].equals("") && !newVals3[i].equals("")) {
					newVals2[i] = "" + (Integer.parseInt(newVals1[i]) + Integer.parseInt(newVals3[i]))/2;
				}
			}
		}

		if (occ.get(patno) - offset > MIN_VISIT_1_234) { // no average for the last visit
			String[] newVals4 = new String[OUT_COLUMNS.length];
			for (int i = 0; i < OUT_COLUMNS.length; i++) {
				newVals4[i] = data.get(index+4)[data.get(index+4).length - OUT_COLUMNS.length+i].trim();
			}
			for (int i=0; i < newVals3.length;i++) {
				if (newVals3[i].equals("")) {
					if (!newVals2[i].equals("") && !newVals4[i].equals("")) {
						newVals3[i] = "" + (Integer.parseInt(newVals2[i]) + Integer.parseInt(newVals4[i]))/2;
					}
				}
			}
		}
		// end massage
		
		for (int i=0; i < outVals.length; i++) {
			if (outVals[i].equals(""))
				isEmpty = true;
		}
		for (int i=0; i < newVals1.length; i++) {
			if (newVals1[i].equals(""))
				isEmpty = true;
		}
		for (int i=0; i < newVals2.length; i++) {
			if (newVals2[i].equals(""))
				isEmpty = true;
		}
		for (int i=0; i < newVals3.length; i++) {
			if (newVals3[i].equals(""))
				isEmpty = true;
		}
		
		if (!isEmpty) 
			ps.println(getArr(row) + "," + getArr(newVals1) + "," + getArr(newVals2) + "," + getArr(newVals3));

		return isEmpty;
	}

	
	public static void main(String[] args) {
		try {
			PPMIDataCleanup p = new PPMIDataCleanup(INPUT_FILE);
			p.processData();
		}
		catch (Throwable th) {
			th.printStackTrace();
		}

	}
	
    public static String getArr(String[] sar) {
    	//String s = Arrays.toString(sar);
    	String s  = sar[0];
    	for (int i=1; i < sar.length; i++) {
    		s += ","  + sar[i];
    	}
        return s;
    }

    private static int indexOf(String[] ar, String key) {
    	for (int i=0; i < ar.length; i++)
    		if (ar[i].equals(key))
    			return i;
    	return -1;
    }
}
