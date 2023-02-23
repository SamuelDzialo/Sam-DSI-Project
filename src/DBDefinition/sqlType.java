package DBDefinition;

public interface sqlType {
    /**
     * Convert an object to a byte array for this specific type
     * @param o the object to convert
     * @return an array of bytes representing the object
     */
    byte[] toByteArray(Object o);

    /**
     * Converts an array of bytes into an object of the corresponding type
     * @param bytes an array of bytes to decode
     * @return an Object made from the bytes
     */
    Object fromByteArray(byte[] bytes);

    /**
     * Check if input from the user matches this type
     * @param str the input
     * @return true if the input if valid, false otherwise
     */
    boolean matchesType(String str);

    /**
     * Get the value of this type from user input
     * @param s the string to convert
     * @return an object made from the string
     */
    Object fromString(String s);

}
