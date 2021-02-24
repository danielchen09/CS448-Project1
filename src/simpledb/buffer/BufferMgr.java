package simpledb.buffer;
import simpledb.file.*;
import simpledb.log.LogMgr;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class BufferMgrBuffMap extends BufferMgr {
    private HashMap<BlockId, Buffer> bufferMap = new HashMap<>();
    private List<Buffer> unpinnedBlocks = new ArrayList<>();
    private int numbuffs;

    public BufferMgrBuffMap(FileMgr fm, LogMgr lm, int numbuffs) {
        this.numAvailable = numbuffs;
        this.numbuffs = numbuffs;
        for (int i = 0; i < numbuffs; i++)
            unpinnedBlocks.add(new Buffer(fm, lm));
    }

    @Override
    protected Buffer tryToPin(BlockId blk) {
        Buffer buff = findExistingBuffer(blk);
        if (buff == null) {
            buff = chooseUnpinnedBuffer();
            if (buff == null)
                return null;
            if (buff.block() != blk) {
                bufferMap.remove(buff.block());
            }
            buff.assignToBlock(blk);
            bufferMap.put(blk, buff);
        }
        if (!buff.isPinned()) {
            unpinnedBlocks.remove(buff);
            numAvailable--;
        }
        buff.pin();
        return buff;
    }

    @Override
    protected Buffer findExistingBuffer(BlockId blk) {
        if (bufferMap.containsKey(blk))
            if (bufferMap.get(blk) != null)
                return bufferMap.get(blk);
            return null;
    }

    @Override
    protected Buffer chooseUnpinnedBuffer() {
        if (unpinnedBlocks.size() > 0) {
            return unpinnedBlocks.remove(0);
        }
        return null;
    }

    @Override
    public int getNumBuffs() {
        return numbuffs;
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
