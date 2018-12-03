package bufmgr;
import global.GlobalConst;
import global.Minibase;
import global.Page;
import global.PageId;
import java.util.HashMap;

public class FrameDesc {

	
	private int page_number;
    private boolean dirty;
    private int pin_count;
    private boolean reference_bit;

    FrameDesc() {
        this.page_number = -1;
        this.dirty = false;
        this.pin_count = 0;
        this.reference_bit = false;
    }
   
    int getPage_number() {
        return this.page_number;
    }
    
    void setpage_number(int pageno) {
        this.page_number = pageno;
    }

   
    void setDirty(boolean toSet) {
        this.dirty = toSet;
    }
   
    boolean getDirty() {
        return dirty;
    }

    
    void increment_pin_count() {
        this.pin_count++;
    }

   
    void decrement_pin_count() {
        if (pin_count > 0) {
            this.pin_count--;
        }
    }

    
    int getPin_count() {
        return this.pin_count;
    }

    
    void setReference_bit(boolean toSet) {
        this.reference_bit = toSet;
    }

    
    boolean getReference_bit() {
        return reference_bit;
    }
}
	

