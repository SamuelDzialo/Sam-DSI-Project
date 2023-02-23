package IO;

import DBDefinition.AttributeSchema;
import DBDefinition.TableSchema;
import DBDefinition.Record;

import java.nio.ByteBuffer;
import java.util.ArrayList;

public class Page {

    private String tableName;
    private ArrayList<Record> records;
    private TableSchema tableSchema;
    private int pkIndex;


    public Page(String tableName, ArrayList<Record> recordList, TableSchema tableSchema, int pkIndex){
        this.tableName = tableName;
        this.records = recordList;
        this.tableSchema = tableSchema;
        this.pkIndex = pkIndex;
    }

    public Page(String tableName, ArrayList<Record> recordList, TableSchema tableSchema){
        this.tableName = tableName;
        this.records = recordList;
        this.tableSchema = tableSchema;
        this.setPk();
    }

    public Page(String tableName, Record r, TableSchema tableSchema){
        this.tableName = tableName;
        this.tableSchema = tableSchema;
        this.records = new ArrayList<>();
        this.records.add(r);
        this.setPk();
    }

    public Page(){
        pkIndex = -1;
    }

    public ArrayList<Record> getRecords(){
        return this.records;
    }
    public ArrayList<AttributeSchema> getSchema(){ return this.tableSchema.getAttrList(); }

    //TODO

    /**
     * Gets a page from a table file
     *
     * @param page_bytes
     * @return
     */
    public static Page getPage(byte[] page_bytes){
       Page page = new Page();

       return page;
    }


    public void setPk(){
        ArrayList<AttributeSchema> schema = this.tableSchema.getAttrList();
        for(int i=0; i<schema.size(); i++){
            if(schema.get(i).getIsPk() == 1){
                pkIndex = i;
            }
        }
    }

    public boolean insertRecord(Record record){
        Object pk = record.getValue(pkIndex);
        AttributeSchema pkAttrib = tableSchema.getAttrList().get(pkIndex);
        if(pkAttrib.isLessThan(this.records.get(this.records.size()-1), pk)){
            return false;
        }
        //Note this method will insert regardless of whether or not a page is full.
        //After an insert the page size should be checked and if too big should be split via the split method
        int insertIndex = 0;
        while(pkAttrib.isLessThan(this.records.get(insertIndex).getValue(pkIndex), pk)){
            insertIndex++;
        }
        this.records.add(insertIndex, record);
        return true;
    }

    public int getPageSize(){
        int sum=0;
        for(Record r: records){
            sum+= r.getRecordsize(this.tableSchema.getAttrList());
        }
        return sum;
    }

    public Page split(){
        int splitIndex = this.records.size()/2;
        if(this.records.size()%2 == 1){
            splitIndex++;
        }
        ArrayList<Record> newRecords = new ArrayList<>();
        for(int i=splitIndex; i<this.records.size(); i++){
            newRecords.add(this.records.remove(splitIndex));
        }
        return new Page(this.tableName, newRecords, this.tableSchema,this.pkIndex);
    }

    // bytebuffer 

    /**
     * computes size of page in bytes
     * @param page_bytes
     * @return size
     */
    public static int computeSize(byte[] page_bytes){
        // for(byte byt : page_bytes){
        //     if(){ // int
                
        //     }
        //     else if(){ // double

        //     }
        //     else if(){ // boolean

        //     }
        //     else if(){ // char(N)

        //     }
        //     else if(){ // varchar(N)

        //     }
        //     else(){
        //         // throw an error
        //     }
        // }
        return 0;
    }

    /**
     * parse bytes
     * @param page_bytes
     * @return
     */
    public static Page parseBytes(byte[] page_bytes, TableSchema tableSchema){
        ByteBuffer buf = ByteBuffer.wrap(page_bytes);
        ArrayList<Record> recordLst = new ArrayList<>();
        int numRecords = buf.getInt();
        for(int i=0; i<numRecords; i++){
            recordLst.add(Record.parseRecord(buf, tableSchema.getAttrList()));
        }
        return new Page(tableSchema.getTableName(), recordLst, tableSchema);
    }

    /**
     * get bytes of page
     * @param page
     * @return
     */
    public static byte[] getBytes(Page page, int pageSize){
        ByteBuffer buf = ByteBuffer.allocate(pageSize);
        buf.putInt(page.records.size());
        for(Record r: page.records){
            ByteBuffer b = r.toByteBuffer(page.getSchema());
            byte[] temp = new byte[b.capacity()];
            b.get(temp);
            buf.put(temp);
        }
        byte[] ret = new byte[pageSize];
        buf.reset();
        buf.get(ret);
        return ret;
    }

}
// writes record