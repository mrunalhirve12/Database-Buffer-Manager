package tests;

import global.Convert;
import global.Minibase;
import global.Page;
import global.PageId;

/**
 * Test suite for the bufmgr layer.
 */
class BMTestExtra extends TestDriver {

  /** The display name of the test suite. */
  private static final String TEST_NAME = "buffer manager EXTRA TESTS";

  /**
   * Test application entry point; runs all tests.
   */
  public static void main(String argv[]) {

    // create a clean Minibase instance.  This requires pinning and unpinning
	// the first page of the database.
	BMTestExtra bmt = new BMTestExtra();
	bmt.create_minibase();

    // run all the test cases
    System.out.println("\n" + "Running " + TEST_NAME + "...");
    boolean status = PASS;
    status &= bmt.test4();
    status &= bmt.test5();

    // display the final results
    System.out.println();
    if (status != PASS) {
      System.out.println("Error(s) encountered during " + TEST_NAME + ".");
    } else {
      System.out.println("All " + TEST_NAME + " completed successfully!");
    }

  } // public static void main (String argv[])

  /**
   * 
   */
  protected boolean test4() {
/*
 * pin few page,
 * write something
 * flush
 * read pages from diskmgr 
 * 
 * Test2
 * pin all to write and unpin dirty
 * Pin all and unpin clean
 * Pin new pages 
 * Pin old pages and read them
 */
	  
	  
    System.out.print("\n  Test 4 tests flushAllPages function\n");

    // Allocate pages 
    boolean status1 = PASS;
    int toAlloc =  Minibase.BufferManager.getNumFrames();
    Page pg = new Page();  //refers to the current frame
    PageId firstPid = new PageId();

    System.out.print("  - Allocate pages\n");
    try {
      firstPid = Minibase.BufferManager.newPage(pg, toAlloc);
    } catch (Exception e) {
      System.err.print("*** Could not allocate " + toAlloc);
      System.err.print(" new pages in the database.\n");
      e.printStackTrace();
      return false;
    }

    
    // unpin that first page... to simplify our loop
    try {
      Minibase.BufferManager.unpinPage(firstPid, UNPIN_CLEAN);
    } catch (Exception e) {
      System.err.print("*** Could not unpin the first new page.\n");
      e.printStackTrace();
      status1 = FAIL;
    }
    
    // Write something on each allocated page.  Pin, write, unpin.
    System.out.print("  - Write something on each one\n");
    PageId pid = new PageId();
    PageId lastPid = new PageId();

    for (pid.pid = firstPid.pid, lastPid.pid = pid.pid + toAlloc; status1 == PASS
        && pid.pid < lastPid.pid; pid.pid = pid.pid + 1) {

      try {
        Minibase.BufferManager.pinPage(pid, pg, PIN_NOOP);
      } catch (Exception e) {
        System.err.print("*** Could not pin new page " + pid.pid + "\n");
        e.printStackTrace();
        return false; //If we can't pin the page, end this test
      }

      // Copy the page number + 99 onto each page. It seems
      // unlikely that this bit pattern would show up there by
      // coincidence.
      int data = pid.pid + 99;
      Convert.setIntValue(data, 0, pg.getData());

      try {
        Minibase.BufferManager.unpinPage(pid, UNPIN_DIRTY);
      } catch (Exception e) {
        status1 = FAIL; //record failure but continue the test
        System.err.print("*** Could not unpin dirty page " + pid.pid + "\n");
        e.printStackTrace();
      }
    }

    System.out.print("  - Now we flush all fames to disk\n");
    Minibase.BufferManager.flushAllFrames();
    
    
    // Read that something back from each page.  Pin, read, unpin.
    // Because there are more pages than frames, disk I/O will happen here.
    System.out.print("  - Make sure pages are written to disk, we Read them by calling the disk manager\n");
    
    for (pid.pid = firstPid.pid; status1 == PASS && pid.pid < lastPid.pid; 
    		pid.pid = pid.pid + 1) {

      try {
    	Minibase.DiskManager.read_page(pid, pg);  
        //Minibase.BufferManager.pinPage(pid, pg, PIN_DISKIO);
      } catch (Exception e) {
        System.err.print("*** Could not read page from disk" + pid.pid + "\n");
        e.printStackTrace();
        return false;  //If we can't pin the page, end this test
      }

      int data = 0;
      data = Convert.getIntValue(0, pg.getData());

      if (status1 == PASS) {
        if (data != (pid.pid) + 99) {
          status1 = FAIL;  //record failure but continue the test
          System.err.print("*** Read wrong data back from page (in disk) " + pid.pid + "\n");
        }
      }
      
    }

    //Free the allocated pages
    if (status1 == PASS) {
      System.out.print("  - Free the allocated pages\n");

      for (pid.pid = firstPid.pid; pid.pid < lastPid.pid; pid.pid = pid.pid + 1) {

        try {
          Minibase.BufferManager.freePage(pid);
        } catch (Exception e) {
          status1 = FAIL;
          System.err.print("*** Error freeing page " + pid.pid + "\n");
          e.printStackTrace();
        }
      }
    }

    if (status1 == PASS)
      System.out.print("  TEST 4 COMPLETED SUCESSFULLY.\n");
    else 
      System.err.print("  TEST 4 COMPLETED WITH ERRORS.\n"); 	

    return status1;

  } // protected boolean test4 ()

  
  protected boolean test5() {

	    System.out.print("\n  Test 5 tests whether BM keeps track of dirty bit correctly\n");

	    // Allocate a number of pages = num of frames - 10
	    boolean status1 = PASS;
	    int toAlloc = Minibase.BufferManager.getNumUnpinned() - 10 ;
	    Page pg = new Page();  //refers to the current frame
	    PageId firstPid = new PageId();
	    PageId SecondGroupfirstPid = new PageId();

	    System.out.print("  - Allocate # of pages = # of frames - 10\n");
	    try {
	      firstPid = Minibase.BufferManager.newPage(pg, toAlloc);
	    } catch (Exception e) {
	      System.err.print("*** Could not allocate " + toAlloc);
	      System.err.print(" new pages in the database.\n");
	      e.printStackTrace();
	      return false;
	    }

	    // unpin that first page... to simplify our loop
	    try {
	      Minibase.BufferManager.unpinPage(firstPid, UNPIN_CLEAN);
	    } catch (Exception e) {
	      System.err.print("*** Could not unpin the first new page.\n");
	      e.printStackTrace();
	      status1 = FAIL;
	    }

	    // Write something on each allocated page.  Pin, write, unpin(dirty).
	    System.out.print("  - Write something on each one\n");
	    PageId pid = new PageId();
	    PageId lastPid = new PageId();

	    for (pid.pid = firstPid.pid, lastPid.pid = pid.pid + toAlloc; status1 == PASS
	        && pid.pid < lastPid.pid; pid.pid = pid.pid + 1) {

	      try {
	        Minibase.BufferManager.pinPage(pid, pg, PIN_NOOP);
	      } catch (Exception e) {
	        System.err.print("*** Could not pin new page " + pid.pid + "\n");
	        e.printStackTrace();
	        return false; //If we can't pin the page, end this test
	      }

	      // Copy the page number + 888 onto each page. It seems
	      // unlikely that this bit pattern would show up there by
	      // coincidence.
	      int data = pid.pid + 888;
	      Convert.setIntValue(data, 0, pg.getData());

	      try {
	        Minibase.BufferManager.unpinPage(pid, UNPIN_DIRTY);
	      } catch (Exception e) {
	        status1 = FAIL; //record failure but continue the test
	        System.err.print("*** Could not unpin dirty page " + pid.pid + "\n");
	        e.printStackTrace();
	      }
	    }

	    // The following loop iterate on all pages with pin and unpin(clean)
	    // Buffer manager should still keep dirty bit = true since all pages were touched in the previous loop 
	    System.out.print("  - Loop with pin and unpin(clean)\n");

	    for (pid.pid = firstPid.pid; status1 == PASS && pid.pid < lastPid.pid; 
	    		pid.pid = pid.pid + 1) {

	      try {
	        Minibase.BufferManager.pinPage(pid, pg, PIN_DISKIO);
	      } catch (Exception e) {
	        System.err.print("*** Could not pin page " + pid.pid + "\n");
	        e.printStackTrace();
	        return false;  //If we can't pin the page, end this test
	      }

	      try {
	        Minibase.BufferManager.unpinPage(pid, UNPIN_CLEAN);
	      } catch (Exception e) {
	        status1 = FAIL;  //record failure but continue the test
	        System.err.print("*** Could not unpin page " + pid.pid + "\n");
	        e.printStackTrace();
	      }
	    }

	    //-----------------------------------
	    //-------------------------------------
	    System.out.print("  - Allocate a second group of pages. This will force the first group to be written to disk\n");
	    try {
	    	SecondGroupfirstPid = Minibase.BufferManager.newPage(pg, toAlloc);
	    } catch (Exception e) {
	      System.err.print("*** Could not allocate " + toAlloc);
	      System.err.print(" new pages in the database.\n");
	      e.printStackTrace();
	      return false;
	    }

	    // unpin that first page... to simplify our loop
	    try {
	      Minibase.BufferManager.unpinPage(SecondGroupfirstPid, UNPIN_CLEAN);
	    } catch (Exception e) {
	      System.err.print("*** Could not unpin the first new page.\n");
	      e.printStackTrace();
	      status1 = FAIL;
	    }

	    // Write something on each allocated page.  Pin, write, unpin(dirty).
	    System.out.print("  - Write something on each page in the second group\n");
	    PageId SecondGrouppid = new PageId();
	    PageId SecondGrouplastPid = new PageId();

	    for (SecondGrouppid.pid = SecondGroupfirstPid.pid, SecondGrouplastPid.pid = SecondGrouppid.pid + toAlloc; status1 == PASS
	        && SecondGrouppid.pid < SecondGrouplastPid.pid; SecondGrouppid.pid = SecondGrouppid.pid + 1) {

	      try {
	        Minibase.BufferManager.pinPage(SecondGrouppid, pg, PIN_NOOP);
	      } catch (Exception e) {
	        System.err.print("*** Could not pin new page " + pid.pid + "\n");
	        e.printStackTrace();
	        return false; //If we can't pin the page, end this test
	      }

	      // Copy the page number + 66666 onto each page. It seems
	      // unlikely that this bit pattern would show up there by
	      // coincidence.

	      int data = SecondGrouppid.pid + 66666;
	      Convert.setIntValue(data, 0, pg.getData());

	      try {
	        Minibase.BufferManager.unpinPage(SecondGrouppid, UNPIN_DIRTY);
	      } catch (Exception e) {
	        status1 = FAIL; //record failure but continue the test
	        System.err.print("*** Could not unpin dirty page " + SecondGrouppid.pid + "\n");
	        e.printStackTrace();
	      }
	    }

	    System.out.print("  - Now try to pin and read the pages from the first group\n");

	    for (pid.pid = firstPid.pid; status1 == PASS && pid.pid < lastPid.pid; 
	    		pid.pid = pid.pid + 1) {

	      try {
	        Minibase.BufferManager.pinPage(pid, pg, PIN_DISKIO);
	      } catch (Exception e) {
	        System.err.print("*** Could not pin page " + pid.pid + "\n");
	        e.printStackTrace();
	        return false;  //If we can't pin the page, end this test
	      }

	      int data = 0;
	      data = Convert.getIntValue(0, pg.getData());

	      if (status1 == PASS) {
	        if (data != (pid.pid) + 888) {
	          status1 = FAIL;  //record failure but continue the test
	          System.err.print("*** Read wrong data back from page " + pid.pid + "\n");
	        }
	      }

	      try {
	        Minibase.BufferManager.unpinPage(pid, UNPIN_CLEAN);
	      } catch (Exception e) {
	        status1 = FAIL;  //record failure but continue the test
	        System.err.print("*** Could not unpin page " + pid.pid + "\n");
	        e.printStackTrace();
	      }
	    }

	    
	    //Free the allocated pages
	    if (status1 == PASS) {
	      System.out.print("  - Free the allocated pages in the first group\n");

	      for (pid.pid = firstPid.pid; pid.pid < lastPid.pid; pid.pid = pid.pid + 1) {

	        try {
	          Minibase.BufferManager.freePage(pid);
	        } catch (Exception e) {
	          status1 = FAIL;
	          System.err.print("*** Error freeing page " + pid.pid + "\n");
	          e.printStackTrace();
	        }
	      }
	    }

	    if (status1 == PASS) {
		      System.out.print("  - Free the allocated pages in the second group\n");

		      for (SecondGrouppid.pid = SecondGroupfirstPid.pid; SecondGrouppid.pid < SecondGrouplastPid.pid; SecondGrouppid.pid = SecondGrouppid.pid + 1) {

		        try {
		          Minibase.BufferManager.freePage(pid);
		        } catch (Exception e) {
		          status1 = FAIL;
		          System.err.print("*** Error freeing page " + SecondGrouppid.pid + "\n");
		          e.printStackTrace();
		        }
		      }
	    }
	    
	    if (status1 == PASS)
	        System.out.print("  TEST 5 COMPLETED SUCESSFULLY.\n");
	      else 
	        System.err.print("  TEST 5 COMPLETED WITH ERRORS.\n"); 	


	    return status1;

	  } // protected boolean test5 ()

} // class BMTest extends TestDriver
