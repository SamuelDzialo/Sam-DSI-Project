package IO;

import DBDefinition.Catalog;

import java.io.RandomAccessFile;
import java.util.ArrayList;

public class BufferManager {
    //private Buffer buffer;
    private Byte[] buffer;

    public BufferManager(int bufferSize) {
        this.buffer = new Byte[bufferSize];
    }

    public void InsertPageInBuffer(Page page){

    }

    //read in page to buffer
    public void readPage(){
        //TODO
    }

    //access page from buffer
    public Page getPage(){
        //TODO
        return null;
    }
}
