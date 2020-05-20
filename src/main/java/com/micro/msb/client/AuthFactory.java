package com.micro.msb.client;

import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;
import java.util.Random;

/**
 *
 * @author yanyan
 */
public class AuthFactory {

    public static synchronized String hash(String data) {
        java.security.MessageDigest digest;
        try {
            digest = java.security.MessageDigest.getInstance("SHA-1");
            digest.update(data.getBytes("UTF-8"));
            return encodeHex(digest.digest());
        } catch (NoSuchAlgorithmException nsae) {
            System.err.println("Failed to load the SHA-1 MessageDigest. Jive will be unable to function normally.");
            return null;
        } catch (UnsupportedEncodingException e) {
            System.err.println(e);
            return null;
        }
    }
    
    public static String encodeHex(byte[] bytes) {
        StringBuilder hex = new StringBuilder(bytes.length * 2);

        for (byte aByte : bytes) {
            if ((aByte & 0xFF) < 16) {
                hex.append("0");
            }
            hex.append(Integer.toString(aByte & 0xFF, 16));
        }

        return hex.toString();
    }
    
    /**
     * Returns an encrypted version of the plain-text password. Encryption is performed
     * using the Blowfish algorithm. The encryption key is stored as the Jive property
     * "passwordKey". If the key is not present, it will be automatically generated.
     *
     * @param password the plain-text password.
     * @param passwordKey
     * @return the encrypted password.
     * @throws UnsupportedOperationException if encryption/decryption is not possible;
     *      for example, during setup mode.
     */    
    public static String encryptPassword(String password,String passwordKey) {
        if (password == null) {
            return null;
        }
        Blowfish cipher = new Blowfish(passwordKey);
        if (cipher == null) {
            throw new UnsupportedOperationException();
        }
        return cipher.encryptString(password);
    }

    /**
     * Returns a decrypted version of the encrypted password. Encryption is performed
     * using the Blowfish algorithm. The encryption key is stored as the Jive property
     * "passwordKey". If the key is not present, it will be automatically generated.
     *
     * @param encryptedPassword the encrypted password.
     * @param passwordKey
     * @return the encrypted password.
     * @throws UnsupportedOperationException if encryption/decryption is not possible;
     *      for example, during setup mode.
     */
    public static String decryptPassword(String encryptedPassword,String passwordKey) {
        if (encryptedPassword == null) {
            return null;
        }
        Blowfish cipher = new Blowfish(passwordKey);
        if (cipher == null) {
            throw new UnsupportedOperationException();
        }
        return cipher.decryptString(encryptedPassword);
    }

    /**
     * Pseudo-random number generator object for use with randomString().
     * The Random class is not considered to be cryptographically secure, so
     * only use these random Strings for low to medium security applications.
     */
    private static Random randGen = new Random();
    
    /**
     * Array of numbers and letters of mixed case. Numbers appear in the list
     * twice so that there is a more equal chance that a number will be picked.
     * We can use the array to get a random number or letter by picking a random
     * array index.
     */
    private static char[] numbersAndLetters = ("0123456789abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ").toCharArray();
    
    /**
     * Returns a random String of numbers and letters (lower and upper case)
     * of the specified length. The method uses the Random class that is
     * built-in to Java which is suitable for low to medium grade security uses.
     * This means that the output is only pseudo random, i.e., each number is
     * mathematically generated so is not truly random.<p>
     * <p/>
     * The specified length must be at least one. If not, the method will return
     * null.
     *
     * @param length the desired length of the random String to return.
     * @return a random String of numbers and letters of the specified length.
     */
    public static String randomString(int length) {
        if (length < 1) {
            return null;
        }
        // Create a char buffer to put random letters and numbers in.
        char[] randBuffer = new char[length];
        for (int i = 0; i < randBuffer.length; i++) {
            randBuffer[i] = numbersAndLetters[randGen.nextInt(71)];
        }
        return new String(randBuffer);
    }    
}
