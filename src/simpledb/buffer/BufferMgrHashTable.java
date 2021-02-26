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

   private HashMap<BlockId, Integer> buffHash = new HashMap<>();

   public BufferMgrHashTable(FileMgr fm, LogMgr lm, int numbuffs) {
      super(fm, lm, numbuffs);
   }

   //implement a hash table for the bufferpool, make searching in bufferpool quicker
   //key that maps to a buffer, key - BlockId, value - index in the bufferpool

   @Override
   protected Buffer tryToPin(BlockId blk) {
      int index = findExistingBuffer2(blk);
      Buffer buff = null;
      if (index == -1) { //if it doesn't contain the key
         index = chooseUnpinnedBuffer2();
         if (index == -1) { //not found
            return null;
         }
         //unpinned index
         buff = bufferpool[index];
         if (buff.block() != blk) {
            buffHash.remove(buff.block());
         }
         buff.assignToBlock(blk);
         buffHash.put(blk, index);
      }
      else {
         buff = bufferpool[index];
      }
      if (!buff.isPinned()) {
         numAvailable--;
      }
      buff.pin();
      return buff;
   }

   protected int findExistingBuffer2(BlockId blk) {
      if (!buffHash.containsKey(blk)) {
         return -1;
      }
      return buffHash.get(blk);
   }

   protected int chooseUnpinnedBuffer2() {
      for (int i = 0; i < bufferpool.length; i++) {
         if (!bufferpool[i].isPinned()) { //isPinned is true when the buffer is currently pinned
            return i;
         }
      }
      return -1;
   }

   @Override
   public synchronized void unpin(Buffer buff) {
      buff.unpin();
      if (!buff.isPinned()) {
         numAvailable++;
         notifyAll();
      }
   }
}
