package org.corfudb.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufProcessor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ScatteringByteChannel;
import java.nio.charset.Charset;

/**
 * Created by mwei on 3/17/16.
 */

@RequiredArgsConstructor
public class AutoCloseableByteBuf extends ByteBuf implements AutoCloseable {

    @Getter
    final ByteBuf buf;

    @Override
    public void close() {
        buf.release();
    }

    @Override
    public int capacity() {
        return buf.capacity();
    }

    @Override
    public ByteBuf capacity(int i) {
        return buf.capacity(i);
    }

    @Override
    public int maxCapacity() {
        return buf.maxCapacity();
    }

    @Override
    public ByteBufAllocator alloc() {
        return buf.alloc();
    }

    @Override
    public ByteOrder order() {
        return buf.order();
    }

    @Override
    public ByteBuf order(ByteOrder byteOrder) {
        return buf.order(byteOrder);
    }

    @Override
    public ByteBuf unwrap() {
        return buf.unwrap();
    }

    @Override
    public boolean isDirect() {
        return buf.isDirect();
    }

    @Override
    public int readerIndex() {
        return buf.readerIndex();
    }

    @Override
    public ByteBuf readerIndex(int i) {
        return buf.readerIndex(i);
    }

    @Override
    public int writerIndex() {
        return buf.writerIndex();
    }

    @Override
    public ByteBuf writerIndex(int i) {
        return buf.writerIndex(i);
    }

    @Override
    public ByteBuf setIndex(int i, int i1) {
        return buf.setIndex(i, i1);
    }

    @Override
    public int readableBytes() {
        return buf.readableBytes();
    }

    @Override
    public int writableBytes() {
        return buf.writableBytes();
    }

    @Override
    public int maxWritableBytes() {
        return buf.maxWritableBytes();
    }

    @Override
    public boolean isReadable() {
        return buf.isReadable();
    }

    @Override
    public boolean isReadable(int i) {
        return buf.isReadable(i);
    }

    @Override
    public boolean isWritable() {
        return buf.isWritable();
    }

    @Override
    public boolean isWritable(int i) {
        return buf.isWritable(i);
    }

    @Override
    public ByteBuf clear() {
        return buf.clear();
    }

    @Override
    public ByteBuf markReaderIndex() {
        return buf.markReaderIndex();
    }

    @Override
    public ByteBuf resetReaderIndex() {
        return buf.resetReaderIndex();
    }

    @Override
    public ByteBuf markWriterIndex() {
        return buf.markWriterIndex();
    }

    @Override
    public ByteBuf resetWriterIndex() {
        return buf.resetWriterIndex();
    }

    @Override
    public ByteBuf discardReadBytes() {
        return buf.discardReadBytes();
    }

    @Override
    public ByteBuf discardSomeReadBytes() {
        return buf.discardSomeReadBytes();
    }

    @Override
    public ByteBuf ensureWritable(int i) {
        return buf.ensureWritable(i);
    }

    @Override
    public int ensureWritable(int i, boolean b) {
        return buf.ensureWritable(i,b);
    }

    @Override
    public boolean getBoolean(int i) {
        return buf.getBoolean(i);
    }

    @Override
    public byte getByte(int i) {
        return buf.getByte(i);
    }

    @Override
    public short getUnsignedByte(int i) {
        return buf.getUnsignedByte(i);
    }

    @Override
    public short getShort(int i) {
        return buf.getShort(i);
    }

    @Override
    public int getUnsignedShort(int i) {
        return buf.getUnsignedShort(i);
    }

    @Override
    public int getMedium(int i) {
        return buf.getMedium(i);
    }

    @Override
    public int getUnsignedMedium(int i) {
        return buf.getUnsignedMedium(i);
    }

    @Override
    public int getInt(int i) {
        return buf.getInt(i);
    }

    @Override
    public long getUnsignedInt(int i) {
        return buf.getUnsignedInt(i);
    }

    @Override
    public long getLong(int i) {
        return buf.getLong(i);
    }

    @Override
    public char getChar(int i) {
        return buf.getChar(i);
    }

    @Override
    public float getFloat(int i) {
        return buf.getFloat(i);
    }

    @Override
    public double getDouble(int i) {
        return buf.getDouble(i);
    }

    @Override
    public ByteBuf getBytes(int i, ByteBuf byteBuf) {
        return buf.getBytes(i, byteBuf);
    }

    @Override
    public ByteBuf getBytes(int i, ByteBuf byteBuf, int i1) {
        return buf.getBytes(i, byteBuf, i1);
    }

    @Override
    public ByteBuf getBytes(int i, ByteBuf byteBuf, int i1, int i2) {
        return buf.getBytes(i, byteBuf, i1, i2);
    }

    @Override
    public ByteBuf getBytes(int i, byte[] bytes) {
        return buf.getBytes(i, bytes);
    }

    @Override
    public ByteBuf getBytes(int i, byte[] bytes, int i1, int i2) {
        return buf.getBytes(i, bytes, i1, i2);
    }

    @Override
    public ByteBuf getBytes(int i, ByteBuffer byteBuffer) {
        return buf.getBytes(i, byteBuffer);
    }

    @Override
    public ByteBuf getBytes(int i, OutputStream outputStream, int i1) throws IOException {
        return buf.getBytes(i, outputStream, i1);
    }

    @Override
    public int getBytes(int i, GatheringByteChannel gatheringByteChannel, int i1) throws IOException {
        return buf.getBytes(i, gatheringByteChannel, i1);
    }

    @Override
    public ByteBuf setBoolean(int i, boolean b) {
        return buf.setBoolean(i, b);
    }

    @Override
    public ByteBuf setByte(int i, int i1) {
        return buf.setByte(i, i1);
    }

    @Override
    public ByteBuf setShort(int i, int i1) {
        return buf.setShort(i, i1);
    }

    @Override
    public ByteBuf setMedium(int i, int i1) {
        return buf.setMedium(i, i1);
    }

    @Override
    public ByteBuf setInt(int i, int i1) {
        return buf.setInt(i, i1);
    }

    @Override
    public ByteBuf setLong(int i, long l) {
        return buf.setLong(i, l);
    }

    @Override
    public ByteBuf setChar(int i, int i1) {
        return buf.setChar(i, i1);
    }

    @Override
    public ByteBuf setFloat(int i, float v) {
        return buf.setFloat(i, v);
    }

    @Override
    public ByteBuf setDouble(int i, double v) {
        return buf.setDouble(i, v);
    }

    @Override
    public ByteBuf setBytes(int i, ByteBuf byteBuf) {
        return buf.setBytes(i, byteBuf);
    }

    @Override
    public ByteBuf setBytes(int i, ByteBuf byteBuf, int i1) {
        return buf.setBytes(i, byteBuf, i1);
    }

    @Override
    public ByteBuf setBytes(int i, ByteBuf byteBuf, int i1, int i2) {
        return buf.setBytes(i, byteBuf, i1, i2);
    }

    @Override
    public ByteBuf setBytes(int i, byte[] bytes) {
        return buf.setBytes(i, bytes);
    }

    @Override
    public ByteBuf setBytes(int i, byte[] bytes, int i1, int i2) {
        return buf.setBytes(i, bytes, i1, i2);
    }

    @Override
    public ByteBuf setBytes(int i, ByteBuffer byteBuffer) {
        return buf.setBytes(i, byteBuffer);
    }

    @Override
    public int setBytes(int i, InputStream inputStream, int i1) throws IOException {
        return buf.setBytes(i, inputStream, i1);
    }

    @Override
    public int setBytes(int i, ScatteringByteChannel scatteringByteChannel, int i1) throws IOException {
        return buf.setBytes(i, scatteringByteChannel, i1);
    }

    @Override
    public ByteBuf setZero(int i, int i1) {
        return buf.setZero(i, i1);
    }

    @Override
    public boolean readBoolean() {
        return buf.readBoolean();
    }

    @Override
    public byte readByte() {
        return buf.readByte();
    }

    @Override
    public short readUnsignedByte() {
        return buf.readUnsignedByte();
    }

    @Override
    public short readShort() {
        return buf.readShort();
    }

    @Override
    public int readUnsignedShort() {
        return buf.readUnsignedShort();
    }

    @Override
    public int readMedium() {
        return buf.readMedium();
    }

    @Override
    public int readUnsignedMedium() {
        return buf.readUnsignedMedium();
    }

    @Override
    public int readInt() {
        return buf.readInt();
    }

    @Override
    public long readUnsignedInt() {
        return buf.readUnsignedInt();
    }

    @Override
    public long readLong() {
        return buf.readLong();
    }

    @Override
    public char readChar() {
        return buf.readChar();
    }

    @Override
    public float readFloat() {
        return buf.readFloat();
    }

    @Override
    public double readDouble() {
        return buf.readDouble();
    }

    @Override
    public ByteBuf readBytes(int i) {
        return buf.readBytes(i);
    }

    @Override
    public ByteBuf readSlice(int i) {
        return buf.readSlice(i);
    }

    @Override
    public ByteBuf readBytes(ByteBuf byteBuf) {
        return buf.readBytes(byteBuf);
    }

    @Override
    public ByteBuf readBytes(ByteBuf byteBuf, int i) {
        return buf.readBytes(byteBuf, i);
    }

    @Override
    public ByteBuf readBytes(ByteBuf byteBuf, int i, int i1) {
        return buf.readBytes(byteBuf, i, i1);
    }

    @Override
    public ByteBuf readBytes(byte[] bytes) {
        return buf.readBytes(bytes);
    }

    @Override
    public ByteBuf readBytes(byte[] bytes, int i, int i1) {
        return buf.readBytes(bytes, i , i1);
    }

    @Override
    public ByteBuf readBytes(ByteBuffer byteBuffer) {
        return buf.readBytes(byteBuffer);
    }

    @Override
    public ByteBuf readBytes(OutputStream outputStream, int i) throws IOException {
        return buf.readBytes(outputStream, i);
    }

    @Override
    public int readBytes(GatheringByteChannel gatheringByteChannel, int i) throws IOException {
        return buf.readBytes(gatheringByteChannel, i);
    }

    @Override
    public ByteBuf skipBytes(int i) {
        return buf.skipBytes(i);
    }

    @Override
    public ByteBuf writeBoolean(boolean b) {
        return buf.writeBoolean(b);
    }

    @Override
    public ByteBuf writeByte(int i) {
        return buf.writeByte(i);
    }

    @Override
    public ByteBuf writeShort(int i) {
        return buf.writeShort(i);
    }

    @Override
    public ByteBuf writeMedium(int i) {
        return buf.writeMedium(i);
    }

    @Override
    public ByteBuf writeInt(int i) {
        return buf.writeInt(i);
    }

    @Override
    public ByteBuf writeLong(long l) {
        return buf.writeLong(l);
    }

    @Override
    public ByteBuf writeChar(int i) {
        return buf.writeChar(i);
    }

    @Override
    public ByteBuf writeFloat(float v) {
        return buf.writeFloat(v);
    }

    @Override
    public ByteBuf writeDouble(double v) {
        return buf.writeDouble(v);
    }

    @Override
    public ByteBuf writeBytes(ByteBuf byteBuf) {
        return buf.writeBytes(byteBuf);
    }

    @Override
    public ByteBuf writeBytes(ByteBuf byteBuf, int i) {
        return buf.writeBytes(byteBuf, i);
    }

    @Override
    public ByteBuf writeBytes(ByteBuf byteBuf, int i, int i1) {
        return buf.writeBytes(byteBuf, i, i1);
    }

    @Override
    public ByteBuf writeBytes(byte[] bytes) {
        return buf.writeBytes(bytes);
    }

    @Override
    public ByteBuf writeBytes(byte[] bytes, int i, int i1) {
        return buf.writeBytes(bytes, i ,i1);
    }

    @Override
    public ByteBuf writeBytes(ByteBuffer byteBuffer) {
        return buf.writeBytes(byteBuffer);
    }

    @Override
    public int writeBytes(InputStream inputStream, int i) throws IOException {
        return buf.writeBytes(inputStream, i);
    }

    @Override
    public int writeBytes(ScatteringByteChannel scatteringByteChannel, int i) throws IOException {
        return buf.writeBytes(scatteringByteChannel, i);
    }

    @Override
    public ByteBuf writeZero(int i) {
        return buf.writeZero(i);
    }

    @Override
    public int indexOf(int i, int i1, byte b) {
        return buf.indexOf(i, i1, b);
    }

    @Override
    public int bytesBefore(byte b) {
        return buf.bytesBefore(b);
    }

    @Override
    public int bytesBefore(int i, byte b) {
        return buf.bytesBefore(i, b);
    }

    @Override
    public int bytesBefore(int i, int i1, byte b) {
        return buf.bytesBefore(i, i1, b);
    }

    @Override
    public int forEachByte(ByteBufProcessor byteBufProcessor) {
        return buf.forEachByte(byteBufProcessor);
    }

    @Override
    public int forEachByte(int i, int i1, ByteBufProcessor byteBufProcessor) {
        return buf.forEachByte(i, i1, byteBufProcessor);
    }

    @Override
    public int forEachByteDesc(ByteBufProcessor byteBufProcessor) {
        return buf.forEachByteDesc(byteBufProcessor);
    }

    @Override
    public int forEachByteDesc(int i, int i1, ByteBufProcessor byteBufProcessor) {
        return buf.forEachByteDesc(i, i1, byteBufProcessor);
    }

    @Override
    public ByteBuf copy() {
        return buf.copy();
    }

    @Override
    public ByteBuf copy(int i, int i1) {
        return buf.copy(i, i1);
    }

    @Override
    public ByteBuf slice() {
        return buf.slice();
    }

    @Override
    public ByteBuf slice(int i, int i1) {
        return buf.slice(i, i1);
    }

    @Override
    public ByteBuf duplicate() {
        return buf.duplicate();
    }

    @Override
    public int nioBufferCount() {
        return buf.nioBufferCount();
    }

    @Override
    public ByteBuffer nioBuffer() {
        return buf.nioBuffer();
    }

    @Override
    public ByteBuffer nioBuffer(int i, int i1) {
        return buf.nioBuffer(i, i1);
    }

    @Override
    public ByteBuffer internalNioBuffer(int i, int i1) {
        return buf.internalNioBuffer(i, i1);
    }

    @Override
    public ByteBuffer[] nioBuffers() {
        return buf.nioBuffers();
    }

    @Override
    public ByteBuffer[] nioBuffers(int i, int i1) {
        return buf.nioBuffers(i, i1);
    }

    @Override
    public boolean hasArray() {
        return buf.hasArray();
    }

    @Override
    public byte[] array() {
        return buf.array();
    }

    @Override
    public int arrayOffset() {
        return buf.arrayOffset();
    }

    @Override
    public boolean hasMemoryAddress() {
        return buf.hasMemoryAddress();
    }

    @Override
    public long memoryAddress() {
        return buf.memoryAddress();
    }

    @Override
    public String toString(Charset charset) {
        return buf.toString(charset);
    }

    @Override
    public String toString(int i, int i1, Charset charset) {
        return buf.toString(i, i1, charset);
    }

    @Override
    public int hashCode() {
        return buf.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof  AutoCloseableByteBuf)
        {
            return o.hashCode() == hashCode();
        }
        return buf.equals(o);
    }

    @Override
    public int compareTo(ByteBuf byteBuf) {
        return buf.compareTo(byteBuf);
    }

    @Override
    public String toString() {
        return buf.toString();
    }

    @Override
    public ByteBuf retain(int i) {
        return buf.retain(i);
    }

    @Override
    public boolean release() {
        return buf.release();
    }

    @Override
    public boolean release(int i) {
        return buf.release(i);
    }

    @Override
    public int refCnt() {
        return buf.refCnt();
    }

    @Override
    public ByteBuf retain() {
        return buf.retain();
    }
}