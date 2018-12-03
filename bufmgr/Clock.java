package bufmgr;

public class Clock {
	 BufMgr buf;
	    public Clock(BufMgr buf) {
	        this.buf = buf;
	    }
	    
	    public int pickVictim() {
	        
	        for (int i = 0; i <= 1; i++) {
	            for (FrameDesc frame : buf.frametab) {
	                
	                if (frame.getPage_number() == -1) {
	                    return buf.pagemap.get(frame.getPage_number());
	                }

	                if (frame.getPin_count() == 0) {
	                    if (frame.getReference_bit()) {
	                        frame.setReference_bit(false); 
	                    } else {
	                        return buf.pagemap.get(frame.getPage_number());
	                    }
	                }
	            }
	        }
	        throw new IllegalStateException();
	    }
}
