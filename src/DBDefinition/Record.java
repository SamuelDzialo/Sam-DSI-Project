package DBDefinition;

import java.nio.ByteBuffer;
import java.util.ArrayList;

public class Record {

    private ArrayList<Object> valuesArr;

    public Record(ArrayList<Object> valuesArr){
        this.valuesArr = valuesArr;
    }

    public ArrayList<Object> getValues() {
        return this.valuesArr;
    }

    public Object getValue(int index){
        return this.valuesArr.get(index);
    }

    public static Record parseRecord(ByteBuffer input, ArrayList<AttributeSchema> attrList){
        ArrayList<Object> newValuesArr = new ArrayList<>();

        for(AttributeSchema attributeSchema: attrList){
            AttributeType currType = attributeSchema.getType();
            Object o = readAttribute(attributeSchema, input);
            newValuesArr.add(o);
        }
        return new Record(newValuesArr);
    }

    public ByteBuffer toByteBuffer(ArrayList<AttributeSchema> attrList){
        int bufferSize = 0;
        for(AttributeSchema attributeSchema: attrList){
            bufferSize += sizeOf(attributeSchema.getType(), attributeSchema);
        }

        ByteBuffer mainBuffer = ByteBuffer.allocate(bufferSize);
        for(int i=0; i<this.valuesArr.size(); i++){
            ByteBuffer tempBuffer = toByteBuffer(attrList.get(i), this.valuesArr.get(i));
            byte[] temp = new byte[tempBuffer.capacity()];
            tempBuffer.get(temp);
            mainBuffer.put(temp);
        }
        mainBuffer.reset();
        return mainBuffer;
    }

    public void printRecord() {
        int i = 1;
        System.out.print("(");
        for(Object value : valuesArr) {
            System.out.print(value);
            if(i < valuesArr.size()) {
                System.out.print(",");
            }
            i++;
        }
        System.out.println(")");
    }

    public static Object readAttribute(AttributeSchema attributeSchema, ByteBuffer input){
        Object retVal = null;
        switch (attributeSchema.getType()){
            case INTEGER-> retVal = input.getInt();
            case DOUBLE -> retVal = input.getDouble();
            case BOOLEAN -> {
                byte b = input.get();
                if(b == 0){
                    retVal = false;
                }else{
                    retVal = true;
                }
            }
            case CHAR -> {
                StringBuilder s = new StringBuilder();
                for(int i=0; i<attributeSchema.getSize(); i++){
                    s.append(input.getChar());
                }
                retVal = s.toString();
            }
            case VARCHAR -> {
                int attribSize = input.getInt();
                StringBuilder s = new StringBuilder();
                for(int i=0; i<attribSize; i++){
                    s.append(input.getChar());
                }
                retVal = s.toString();
            }
            //TODO for unknown is null the best type to store?
        }
        return retVal;
    }

    public ByteBuffer toByteBuffer(AttributeSchema attributeSchema, Object o){
        ByteBuffer buf;
        switch(attributeSchema.getType()){
            case INTEGER -> {
                buf = ByteBuffer.allocate(4);
                buf.putInt((int) o);
            }
            case DOUBLE -> {
                buf = ByteBuffer.allocate(8);
                buf.putDouble((double) o);
            }
            case BOOLEAN -> {
                buf = ByteBuffer.allocate(1);
                buf.put((boolean) o ? (byte)1 : (byte)0);
            }
            case CHAR -> {
                buf = ByteBuffer.allocate(2*attributeSchema.getSize());
                String s = (String) o;
                for(int i=0; i<attributeSchema.getSize(); i++){
                    buf.putChar(s.charAt(i));
                }
            }
            case VARCHAR -> {
                String s = (String) o;
                int len = s.length();
                buf = ByteBuffer.allocate(len*2+4);
                buf.putInt(len);
                for(int i=0; i<len; i++){
                    buf.putChar(s.charAt(i));
                }
            }
            default -> buf = null;
        }
        if(buf != null){
            buf.reset();
        }
        return buf;
    }

    public int sizeOf(AttributeType type, AttributeSchema attributeSchema){
        int retVal;
        switch (type){
            case INTEGER -> retVal = Integer.BYTES;
            case DOUBLE -> retVal = Double.BYTES;
            case BOOLEAN -> retVal = 1;
            case CHAR -> retVal = Character.BYTES*attributeSchema.getSize();
            case VARCHAR -> retVal = Character.BYTES*attributeSchema.getSize() + 4;
            default -> retVal = 0;
        }
        return retVal;
    }

    public int getRecordsize(ArrayList<AttributeSchema> attributeSchemas){
        int sum=0;
        for(int i=0; i<valuesArr.size(); i++){
            if(valuesArr.get(i) != null){
                sum += sizeOf(attributeSchemas.get(i).getType(), attributeSchemas.get(i));
            }
        }
        return sum;
    }
}
