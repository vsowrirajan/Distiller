package com.mapr.distiller.server.producers.raw;

import com.mapr.distiller.server.queues.RecordQueue;
import com.mapr.distiller.server.recordtypes.MfsGutsRecord;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.util.regex.Pattern;

public class MfsGutsStdoutRecordProducer extends Thread{
	RecordQueue outputQueue;
	BufferedReader stdout;
	boolean shouldExit;
	
	public MfsGutsStdoutRecordProducer(InputStream stdout, RecordQueue outputQueue){
		this.stdout = new BufferedReader(new InputStreamReader(stdout));
		this.outputQueue = outputQueue;
		this.shouldExit = false;
	}
	
	public void requestExit(){
		shouldExit=true;
		try {
			stdout.close();
		} catch (Exception e) {}
	}
	
	public boolean isExpectedHeader(String line){
		if (line.equals("  rpc   lpc  rget  rgetR  tgetR  cget   vcM   vcL   vcH  bget    sg   spg  bskp  rput  rputR  tputR  cput   rsf  bucketWr  fl  ffl  sfl mcom fcom ccom  scr spcr   rsc    rscR  csc   bsc   ssc  spsc    spscR  ldbr  blkr  raSg  raSp  nAdv  raBl  rinc  cinc  rchk  rapp  rtlk  ctlk rbulkb rbulks rim   rip rom   rop rob rocm  cr  cdi  rdi  lkp   sl   vl   ul  dvl   ga   sa   rd  rdp   rl    r    w   sf   td  ksi  ksd   hb    cm  chit  ra   cp   uc   cf  ras  ucs  ses   pr   sr   ow   rr  vsr  raN  raM  raL  raJ  adU   shmem     write   lwrite   bwrite      read    lread  ikv  rkv  qi qd bq aq    di  ic    dd dc   dm mc bc  fc   bl flu icc  of edf       ior      iow   lf lff ls ilw lsc ltxn gs   bch   bcm   blb   blu  bin  bde bre bsp bme bco   rc  irc   brd  nrr  nbr  tbr thc  thtm  msgs   kbs res msgr  kbr "))
			return true;
		return false;
	}
	
	private boolean generateMfsGutsRecord(RecordQueue outputQueue, String producerName, String line) {
		return MfsGutsRecord.produceRecord(outputQueue, producerName, line);
	}
	
	public boolean isHeader(String line){
		return line.matches(".*[a-z].*");
	}
	
	public boolean isSample(String line){
		return !isHeader(line) && Pattern.matches(".*[0-9].*", line);
	}
	
	public void run() {
		//String myId = "MfsGuts" + "#1000#" + System.identityHashCode(this);
		String myId = "MfsGutsRecordProducer";
		boolean verifiedHeader = false;
		while(!shouldExit){
			String line = null;
			try {
				line = stdout.readLine();
			} catch (Exception e) {
				System.err.println("ERROR: Could not read from MFS guts stdout");
				break;
			}
			if(line != null){
				if(isHeader(line)){
					//This is all a hack... hard coded header and all.  See MapR bug 19735.
					if(!verifiedHeader){
						if(isExpectedHeader(line)){
							verifiedHeader = true;
						} else {
							System.err.println("ERROR: MFS guts did not produce the expected header");
							break;
						}
					}
				} else if(isSample(line)){
					if(verifiedHeader) {
						try {
							generateMfsGutsRecord(outputQueue, myId, line);
						} catch (Exception e){
							System.err.println("Failed to produce a MfsGutsRecord");
							e.printStackTrace();
							break;
						}
					} else {
						//Somehow, we have a sample line but no header line was found.
						//We can't parse a sample line without knowing the header.
						break;
					}
				}
				//else skip it, because guts prints blank lines for whatever reason...
			} else {
				System.err.println("ERROR: Could not read from MFS guts stdout.");
				break;
			}
		}
		try {
			stdout.close();
		} catch (Exception e){}
	}
}
