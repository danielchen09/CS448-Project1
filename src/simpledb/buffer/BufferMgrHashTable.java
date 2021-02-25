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
      bufferpool = new Buffer[numbuffs];
      buffHash = new HashMap<>(numbuffs);
      for(int i = 0; i < numbuffs; i++){
         bufferpool[i] = new Buffer(fm, lm);
         //at the same time when we put buffer into bufferpool, we store key value pair into the hashmap
         buffHash.put(bufferpool[i].block(), i);
      }
   }

   //implement a hash table for the bufferpool, make searching in bufferpool quicker
   //key that maps to a buffer, key - BlockId, value - index in the bufferpool

   protected Buffer findExistingBuffer(BlockId blk){
      //getting the index of bufferpool through buffHash.get(blk)
      //get put the value (index) to get to certain place in the array
      if (buffHash.get(blk) == null || blk == null){
         return null;
      }
      return bufferpool[buffHash.get(blk)];
   }

//   protected Buffer chooseUnpinnedBuffer(){
//      for (Integer i: buffHash.values()){
//         if(!bufferpool[i].isPinned()){
//            return bufferpool[i];
//         }
//      }
//      return null;
//   }
}
