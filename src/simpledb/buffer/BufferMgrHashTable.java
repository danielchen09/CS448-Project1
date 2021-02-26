package simpledb.buffer;

import simpledb.file.BlockId;
import simpledb.file.FileMgr;
import simpledb.log.LogMgr;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class BufferMgrHashTable extends BufferMgr{
    private HashMap<BlockId, Integer> bufferMap = new HashMap<>();

    public BufferMgrHashTable(FileMgr fm, LogMgr lm, int numbuffs) {
        super(fm, lm, numbuffs);
    }

    @Override
    protected Buffer tryToPin(BlockId blk) {
        int index = findExistingBuffer2(blk);
        Buffer buff = null;
        if (index == -1) {
            index = chooseUnpinnedBuffer2();
            if (index == -1)
                return null;
            buff = bufferpool[index];
            if (buff.block() != blk) {
                bufferMap.remove(buff.block());
            }
            buff.assignToBlock(blk);
            bufferMap.put(blk, index);
        } else {
            buff = bufferpool[index];
        }
        if (!buff.isPinned()) {
            numAvailable--;
        }
        buff.pin();
        return buff;
    }

    protected int findExistingBuffer2(BlockId blk) {
        if (!bufferMap.containsKey(blk)) {
            return -1;
        }
        return bufferMap.get(blk);
    }

    protected int chooseUnpinnedBuffer2() {
        for (int i = 0; i < bufferpool.length; i++) {
            if (!bufferpool[i].isPinned()) {
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
