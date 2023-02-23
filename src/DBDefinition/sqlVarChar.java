package DBDefinition;

public class sqlVarChar implements sqlType{
    private final int maxSize;
    public sqlVarChar(int size){
        this.maxSize = size;
    }

    @Override
    public boolean matchesType(String str) {
        return str.length() <= maxSize;
    }

    @Override
    public Object fromByteArray(byte[] bytes) {
        //Building a string from a byte array
        //Get the size, it is always the first byte
        int size = bytes[0];
        String result = "";
        for(int i=1; i<size+1; i++){
            //Convert each byte to a char
            char c = (char) bytes[i];
            result = result.concat(Character.toString(c));
        }
        return result;
    }

    @Override
    public byte[] toByteArray(Object o) {
        //Make a byte array from a string
        String toEncode = (String) o;
        int len = toEncode.length();
        byte[] returnBytes = new byte[len+1];
        //Store the length as the first byte
        returnBytes[0] = (byte) len;
        for(int i=0; i<len; i++){
            returnBytes[i+1] = (byte) toEncode.charAt(i);
        }
        return returnBytes;
    }

    @Override
    public Object fromString(String s) {
        //May seem redundant, but other type require parsing
        //so this classes needs to implement fromString too
        return s;
    }
}
