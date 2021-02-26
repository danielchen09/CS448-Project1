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
   private List<Buffer> unpinnedBlocks = new ArrayList<>();
   private int numbuffs;

   public BufferMgrHashTable(FileMgr fm, LogMgr lm, int numbuffs) {
      super(fm, lm, numbuffs);
      for(int i = 0; i < bufferpool.length; i++){
//         bufferpool[i] = new Buffer(fm, lm);
         unpinnedBlocks.add(new Buffer(fm, lm));
//         buffHash.put(bufferpool[i].block(), i);
      }
   }

   //implement a hash table for the bufferpool, make searching in bufferpool quicker
   //key that maps to a buffer, key - BlockId, value - index in the bufferpool

   protected Buffer findExistingBuffer(BlockId blk){
      //getting the index of bufferpool through buffHash.get(blk)
      //get put the value (index) to get to certain place in the array
      if(buffHash.containsKey(blk)){
         if(buffHash.get(blk) != null){
            return bufferpool[buffHash.get(blk)];
         }
      }
      return null;
   }

   @Override
   protected Buffer tryToPin(BlockId blk){
      Buffer buff = findExistingBuffer(blk); //existing buffer with the blockid
      int index = 0;
      if (buff == null) { //if the buffer is null
         buff = chooseUnpinnedBuffer(); //choose another buffer which is unpinned(available)
         if (buff == null) //if that buffer is again null
            return null; //no available buffers
         if (buff.block() != blk) {
            buffHash.remove(buff.block());
         }
         buff.assignToBlock(blk); //assign new blockid to the buffer, this is when we are getting blockid
         for(int i = 0; i < bufferpool.length; i++){
            if(bufferpool[i].equals(buff)){
               index = i;
               break;
            }
         }
         buffHash.put(blk, index); //update the hash table
      }
      if (!buff.isPinned()) { //when it's pinned
         unpinnedBlocks.remove(buff);
         numAvailable--;
      }
      buff.pin();
      return buff;
   }

   @Override
   protected Buffer chooseUnpinnedBuffer() {
      if (unpinnedBlocks.size() > 0) {
         return unpinnedBlocks.remove(0);
      }
      return null;
   }

   @Override
   public synchronized void unpin(Buffer buff) {
      buff.unpin();
      if (!buff.isPinned()) {
         numAvailable++;
         unpinnedBlocks.add(buff);
         notifyAll();
      }
   }
}
