package com.fedex.intl.cd.utils;

import java.security.InvalidKeyException;
import java.security.Key;
import java.security.NoSuchAlgorithmException;
import javax.annotation.PostConstruct;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.KeyGenerator;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.SecretKeySpec;
import javax.xml.bind.DatatypeConverter;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class CryptoUtils {

  private Cipher cipher;
  private Key secretKey;

  @Value("${encryption.seed}")
  private String seed;

  @PostConstruct
  public void setup() throws NoSuchAlgorithmException, NoSuchPaddingException {
    KeyGenerator keyGenerator = KeyGenerator.getInstance("AES");
    keyGenerator.init(128);
    secretKey = new SecretKeySpec(seed.getBytes(), "AES");
    cipher = Cipher.getInstance("AES");
  }

  public String getEncryptedString(String plainText) throws Exception {
    try {
      cipher.init(Cipher.ENCRYPT_MODE,
        secretKey);
      byte[] encryptedBytes = cipher.doFinal(plainText.getBytes());
      return DatatypeConverter.printBase64Binary(encryptedBytes);
    } catch (InvalidKeyException | IllegalBlockSizeException | BadPaddingException e) {
      throw new Exception(e.getMessage());
    }
  }

  public String getDecryptedString(String encryptedText) throws Exception {
    byte[] encryptedTextAsBytes = DatatypeConverter.parseBase64Binary(encryptedText);
    try {
      cipher.init(Cipher.DECRYPT_MODE,
        secretKey);
      return new String(cipher.doFinal(encryptedTextAsBytes));
    } catch (InvalidKeyException | IllegalBlockSizeException | BadPaddingException e) {
      throw new Exception(e.getMessage());
    }
  }
}
