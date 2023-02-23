package DBDefinition;

public class sqlInt implements sqlType{
    @Override
    public boolean matchesType(String str) {
        //Only one . is allowed, keep a flag for it
        boolean foundDecimal = false;
        for(int i=0; i<str.length(); i++){
            //Check if every char is a digit
            if(!Character.isDigit(str.charAt(i))){
                //Check if this is the first decimal point found
                if(!foundDecimal && str.charAt(i) == '.'){
                    foundDecimal = true;
                    //One decimal is allowed, continue to skip the return false
                    continue;
                }
                return false;
            }
        }
        return true;
    }

    @Override
    public byte[] toByteArray(Object o) {
        Integer b = (Integer) o;
        byte[] returnVal = new byte[1];
        returnVal[0] = b.byteValue();
        return returnVal;
    }

    @Override
    public Object fromByteArray(byte[] bytes) {
        byte b = bytes[0];
        return (int) b;
    }

    @Override
    public Object fromString(String s) {
        return Integer.parseInt(s);
    }
}
