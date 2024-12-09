package org.example.other;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Base64;

public class Guard {
    private static String KEY = "eE2A7edUAyt42+V3kQFv9g==";

    public static String encrypt(String data) throws Exception {
        SecretKey secretKey = stringToKey(KEY);
        Cipher cipher = Cipher.getInstance("AES");
        cipher.init(Cipher.ENCRYPT_MODE, secretKey);
        byte[] encryptedBytes = cipher.doFinal(data.getBytes());
        return Base64.getEncoder().encodeToString(encryptedBytes);
    }

    public static String decrypt(String encryptedData) throws Exception {
        SecretKey secretKey = stringToKey(KEY);
        Cipher cipher = Cipher.getInstance("AES");
        cipher.init(Cipher.DECRYPT_MODE, secretKey);
        byte[] decodedBytes = Base64.getDecoder().decode(encryptedData);
        byte[] decryptedBytes = cipher.doFinal(decodedBytes);
        return new String(decryptedBytes);
    }

    public static String keyToString(SecretKey secretKey) {
        return Base64.getEncoder().encodeToString(secretKey.getEncoded());
    }

    public static SecretKey stringToKey(String keyString) {
        byte[] decodedKey = Base64.getDecoder().decode(keyString);
        return new SecretKeySpec(decodedKey, 0, decodedKey.length, "AES");
    }

    public static void insertCredsToCredentialsFile(String encryptedCreds) throws Exception {
        String creds = decrypt(encryptedCreds);

        String filePath = "/root/.aws/credentials";
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath))) {
            writer.write(creds);
        }
    }

    public static void main(String[] args) {
        String filePath = "C:\\Users\\hagai\\.aws\\credentials";
        try {
            String fileContent = new String(Files.readAllBytes(Paths.get(filePath)));
            String encrypted = encrypt(fileContent);
            System.out.println(encrypted);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
