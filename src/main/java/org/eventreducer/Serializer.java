package org.eventreducer;

import com.googlecode.cqengine.IndexedCollection;

import java.nio.ByteBuffer;
import java.util.UUID;

public abstract class Serializer<T extends Serializable> {
    public abstract byte[] hash();

    public abstract void configureIndices(IndexFactory indexFactory) throws IndexFactory.IndexNotSupported;
    public abstract void index(IndexFactory indexFactory, T o);
    public abstract IndexedCollection<T> getIndex(IndexFactory indexFactory);

    public abstract String toString(T serializable);

    public abstract void serialize(T entity, ByteBuffer buffer);
    public abstract T deserialize(ByteBuffer buffer);
    public abstract int size(T entity);

    protected void serialize(byte b, ByteBuffer buffer) {
        buffer.put(b);
    }

    protected byte deserialize(byte b, ByteBuffer buffer) {
        return buffer.get();
    }

    protected int size(byte b) {
        return 1;
    }

    protected void serialize(byte[] b, ByteBuffer buffer) {
        buffer.putInt(b.length);
        buffer.put(b);
    }

    protected byte[] deserialize(byte[] b, ByteBuffer buffer) {
        int len = buffer.getInt();
        byte[] buf = new byte[len];
        buffer.get(buf);
        return buf;
    }

    protected int size(byte[] b) {
        return b.length + 4;
    }

    protected void serialize(short s, ByteBuffer buffer) {
        buffer.putShort(s);
    }

    protected short deserialize(short s, ByteBuffer buffer) {
        return buffer.getShort();
    }

    protected int size(short s) {
        return 2;
    }

    protected void serialize(short[] s, ByteBuffer buffer) {
        buffer.putInt(s.length);
        for (short v : s) {
            buffer.putShort(v);
        }
    }

    protected short[] deserialize(short[] s, ByteBuffer buffer) {
        int len = buffer.getInt();
        short[] arr = new short[len];
        for (int i = 0; i < len; i++) {
            arr[i] = buffer.getShort();
        }
        return arr;
    }

    protected int size(short[] s) {
        return 2*s.length + 4;
    }


    protected void serialize(int i, ByteBuffer buffer) {
        buffer.putInt(i);
    }

    protected int deserialize(int i, ByteBuffer buffer) {
        return buffer.getInt();
    }

    protected int size(int i) {
        return 4;
    }

    protected void serialize(int[] i, ByteBuffer buffer) {
        buffer.putInt(i.length);
        for (int v : i) {
            buffer.putInt(v);
        }
    }

    protected int[] deserialize(int[] ints, ByteBuffer buffer) {
        int len = buffer.getInt();
        int[] arr = new int[len];
        for (int i = 0; i < len; i++) {
            arr[i] = buffer.getInt();
        }
        return arr;
    }

    protected int size(int[] i) {
        return 4*i.length + 4;
    }


    protected void serialize(long l, ByteBuffer buffer) {
        buffer.putLong(l);
    }

    protected long deserialize(long l, ByteBuffer buffer) {
        return buffer.getLong();
    }

    protected int size(long l) {
        return 8;
    }

    protected void serialize(long[] l, ByteBuffer buffer) {
        buffer.putInt(l.length);
        for (long v : l) {
            buffer.putLong(v);
        }
    }

    protected long[] deserialize(long[] l, ByteBuffer buffer) {
        int len = buffer.getInt();
        long[] arr = new long[len];
        for (int i = 0; i < len; i++) {
            arr[i] = buffer.getLong();
        }
        return arr;
    }

    protected int size(long[] l) {
        return 8*l.length + 4;
    }


    protected void serialize(float f, ByteBuffer buffer) {
        buffer.putFloat(f);
    }

    protected float deserialize(float f, ByteBuffer buffer) {
        return buffer.getFloat();
    }

    protected int size(float f) {
        return 4;
    }

    protected void serialize(float[] f, ByteBuffer buffer) {
        buffer.putInt(f.length);
        for (float v : f) {
            buffer.putFloat(v);
        }
    }

    protected float[] deserialize(float[] f, ByteBuffer buffer) {
        int len = buffer.getInt();
        float[] arr = new float[len];
        for (int i = 0; i < len; i++) {
            arr[i] = buffer.getFloat();
        }
        return arr;
    }

    protected int size(float[] f) {
        return 4*f.length + 4;
    }


    protected void serialize(double d, ByteBuffer buffer) {
        buffer.putDouble(d);
    }

    protected double deserialize(double d, ByteBuffer buffer) {
        return buffer.getDouble();
    }

    protected int size(double d) {
        return 8;
    }

    protected void serialize(double[] d, ByteBuffer buffer) {
        buffer.putInt(d.length);
        for (double v : d) {
            buffer.putDouble(v);
        }
    }

    protected double[] deserialize(double[] d, ByteBuffer buffer) {
        int len = buffer.getInt();
        double[] arr = new double[len];
        for (int i = 0; i < len; i++) {
            arr[i] = buffer.getDouble();
        }
        return arr;
    }

    protected int size(double[] d) {
        return 8*d.length + 4;
    }


    protected void serialize(boolean b, ByteBuffer buffer) {
        buffer.put((byte) (b ? 1 : 0));
    }

    protected boolean deserialize(boolean b, ByteBuffer buffer) {
        return buffer.get() == 1;
    }

    protected int size(boolean b) {
        return 1;
    }

    protected void serialize(boolean[] b, ByteBuffer buffer) {
        buffer.putInt(b.length);
        for (boolean v : b) {
            buffer.put((byte) (v ? 1 : 0));
        }
    }

    protected boolean[] deserialize(boolean[] b, ByteBuffer buffer) {
        int len = buffer.getInt();
        boolean[] arr = new boolean[len];
        for (int i = 0; i < len; i++) {
            arr[i] = buffer.get() == 1;
        }
        return arr;
    }

    protected int size(boolean[] b) {
        return b.length + 4;
    }


    protected void serialize(char c, ByteBuffer buffer) {
        buffer.putChar(c);
    }

    protected char deserialize(char c, ByteBuffer buffer) {
        return buffer.getChar();
    }

    protected int size(char c) {
        return 1;
    }

    protected void serialize(char[] c, ByteBuffer buffer) {
        buffer.putInt(c.length);
        for (char v : c) {
            buffer.putChar(v);
        }
    }

    protected char[] deserialize(char[] c, ByteBuffer buffer) {
        int len = buffer.getInt();
        char[] arr = new char[len];
        for (int i = 0; i < len; i++) {
            arr[i] = buffer.getChar();
        }
        return arr;
    }

    protected int size(char[] c) {
        return c.length + 4;
    }

    protected void serialize(String s, ByteBuffer buffer) {
        if (s == null) {
            buffer.putInt(0);
        } else {
            buffer.putInt(s.length());
            buffer.put(s.getBytes());
        }
    }

    protected String deserialize(String s, ByteBuffer buffer) {
        int len = buffer.getInt();
        byte[] buf = new byte[len];
        buffer.get(buf);
        return new String(buf);
    }

    protected int size(String s) {
        if (s == null) {
            return 4;
        }
        return s.getBytes().length + 4;
    }

    protected void serialize(String s[], ByteBuffer buffer) {
        buffer.putInt(s.length);
        for (String v : s) {
            byte[] bytes = v.getBytes();
            buffer.putInt(bytes.length);
            buffer.put(bytes);
        }
    }

    protected String[] deserialize(String[] s, ByteBuffer buffer) {
        int len = buffer.getInt();
        String[] arr = new String[len];
        for (int i = 0; i < len; i++) {
            arr[i] = deserialize(s[i], buffer);
        }
        return arr;
    }

    protected int size(String[] s) {
        int sz = 0;
        for (String v : s) {
            sz += size(v);
        }
        return sz + 4;
    }


    protected void serialize(UUID uuid, ByteBuffer buffer) {
        if (uuid == null) {
            serialize(new UUID(0, 0), buffer);
        } else {
            buffer.putLong(uuid.getMostSignificantBits());
            buffer.putLong(uuid.getLeastSignificantBits());
        }
    }


    protected UUID deserialize(UUID uuid, ByteBuffer buffer) {
        long most = buffer.getLong();
        long least = buffer.getLong();
        return new UUID(most, least);
    }

    protected int size(UUID uuid) {
        return 16;
    }


    protected void serialize(UUID uuid[], ByteBuffer buffer) {
        buffer.putInt(uuid.length);
        for (UUID v : uuid) {
            if (uuid == null) {
                serialize(new UUID(0, 0), buffer);
            } else {
                buffer.putLong(v.getMostSignificantBits());
                buffer.putLong(v.getLeastSignificantBits());
            }
        }
    }

    protected UUID[] deserialize(UUID[] uuid, ByteBuffer buffer) {
        int len = buffer.getInt();
        UUID[] arr = new UUID[len];
        for (int i = 0; i < len; i++) {
            arr[i] = deserialize(uuid[i], buffer);
        }
        return arr;
    }

    protected int size(UUID[] uuid) {
        return 16*uuid.length + 4;
    }


}
