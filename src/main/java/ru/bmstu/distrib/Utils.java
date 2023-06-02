package ru.bmstu.distrib;

/**
 * @author vlad333rrty
 */
public class Utils {
    public static byte[] concat(byte[] a, byte[] b) {
        byte[] res = new byte[a.length + b.length];
        System.arraycopy(a, 0, res, 0 ,a.length);
        System.arraycopy(b, 0, res, a.length, b.length);
        return res;
    }

    public static byte[] intToByteArray(int value) {
        return new byte[] {
                (byte)(value >> 24),
                (byte)(value >> 16),
                (byte)(value >> 8),
                (byte)value };
    }
}
