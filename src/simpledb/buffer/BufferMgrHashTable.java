package simpledb.buffer;

import simpledb.file.*;
import simpledb.log.LogMgr;

import java.util.*;

public class BufferMgrHashTable extends BufferMgr{

   /**
    * Creates a buffer manager having the specified number
    * of buffer slots.
    * This constructor depends on a {@link FileMgr} and
    * {@link LogMgr LogMgr} object.
    *
    * @param fm
    * @param lm
    * @param numbuffs the number of buffer slots to allocate
    */

   private HashMap<BlockId, Integer> buffHash;

   public BufferMgrHashTable(FileMgr fm, LogMgr lm, int numbuffs) {
      super(fm, lm, numbuffs);
      buffHash = new HashMap<>(numbuffs);
   }

   //implement a hash table for the bufferpool, make searching in bufferpool quicker, O(1)
   //key that maps to a buffer, key - BlockId, value - index in the bufferpool

   protected Buffer findExistingBuffer(BlockId blk){
      Integer i = buffHash.get(blk); //getting the index of bufferpool
      return bufferpool[i];
   }

   protected Buffer chooseUnpinnedBuffer(){
      for (Integer i: buffHash.values()){
         if(!bufferpool[i].isPinned()){
            return bufferpool[i];
         }
      }
      return null;
   }
}
