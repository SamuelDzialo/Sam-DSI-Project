package DBDefinition;

import java.io.*;

public class AttributeSchema {

    private String attrName;
    private AttributeType type;
    private int isPK;
    private int size;

    public AttributeSchema(String attrName, String type, int isPK, int size) {
        this.attrName = attrName;
        this.isPK = isPK;
        this.size = size;


        switch (type) {
            case "integer" -> this.type = AttributeType.INTEGER;
            case "double" -> this.type = AttributeType.DOUBLE;
            case "boolean" -> this.type = AttributeType.BOOLEAN;
            case "char" -> this.type = AttributeType.CHAR;
            case "varchar" -> this.type = AttributeType.VARCHAR;
        }
    }

    public AttributeSchema() {
        this.attrName = "";
        this.isPK = -1;
        this.size = -1;
        this.type = AttributeType.UNKNOWN;
    }

    public AttributeType getType() {
        return type;
    }

    public int getSize() {
        return size;
    }

    public String getAttrName() {
        return attrName;
    }

    public void displaySchema() {
        switch (type) {
            case INTEGER -> System.out.print("\t"+attrName+":integer");
            case DOUBLE -> System.out.print("\t"+attrName+":double");
            case BOOLEAN -> System.out.print("\t"+attrName+":boolean");
            case CHAR -> System.out.print("\t"+attrName+":char("+size+")");
            case VARCHAR -> System.out.print("\t"+attrName+":varchar("+size+")");
        }

        if(isPK == 1) {
            System.out.print(" primarykey");
        }
    }
    public void readFromDisk(RandomAccessFile catalogReader) {
        try {
            int attrNameLength = catalogReader.readInt();
            char[] attrNameArr = new char[attrNameLength];
            for(int i = 0; i < attrNameLength; i++) {
                attrNameArr[i] = catalogReader.readChar();
            }
            this.attrName = new String(attrNameArr);
            int typeNum = catalogReader.readInt();

            switch (typeNum) {
                case 0 -> this.type = AttributeType.INTEGER;
                case 1 -> this.type = AttributeType.DOUBLE;
                case 2 -> this.type = AttributeType.BOOLEAN;
                case 3 -> this.type = AttributeType.CHAR;
                case 4 -> this.type = AttributeType.VARCHAR;
                default -> throw new RuntimeException();
            }

            this.size = catalogReader.readInt();
            this.isPK = catalogReader.readInt();
        } catch (Exception e) {
            System.out.println("Error reading attributes from table in schema.");
        }
    }

    public void writeToDisk(RandomAccessFile catalogWriter){
        try {
            catalogWriter.writeInt(attrName.length());
            for(int i = 0; i < attrName.length(); i++) {
                catalogWriter.writeChar(attrName.charAt(i));
            }
            switch (type) {
                case INTEGER -> catalogWriter.writeInt(0);
                case DOUBLE -> catalogWriter.writeInt(1);
                case BOOLEAN -> catalogWriter.writeInt(2);
                case CHAR -> catalogWriter.writeInt(3);
                case VARCHAR -> catalogWriter.writeInt(4);
                default -> throw new RuntimeException();
            }
            catalogWriter.writeInt(size);
            if(this.isPK == 1) {
                catalogWriter.writeInt(1);
            }
            else {
                catalogWriter.writeInt(0);
            }
        } catch (Exception e) {
            System.out.println("Error writing attributes to catalog");
        }
    }

    public int getIsPk(){
        return isPK;
    }

    public boolean isLessThan(Object o1, Object o2){
        boolean ret;
        switch (type){
            case INTEGER -> ret = (Integer)o1 < (Integer)o2;
            case DOUBLE -> ret = (Double)o1 < (Double) o2;
            case CHAR, VARCHAR -> ret = ((String)o1).compareTo((String)o2) < 0;
            default -> throw new RuntimeException();
        }
        return ret;
    }
}

