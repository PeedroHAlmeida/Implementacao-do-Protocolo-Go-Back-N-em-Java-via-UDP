import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class Packet {
    static final byte TYPE_DATA      = 0;
    static final byte TYPE_ACK       = 1;
    static final byte TYPE_HANDSHAKE = 2;
    static final byte TYPE_FIN       = 3;

    static final int MAX_DATA    = 1024;
    // tipo(1) + num_seq(4) + num_ack(4) + tamanho_dados(2) + checksum(4) = 15 bytes
    static final int HEADER_SIZE = 15;
    static final int MAX_PKT     = HEADER_SIZE + MAX_DATA;

    final byte   tipo;
    final int    numSeq;
    final int    numAck;
    final byte[] dados;

    private int storedChecksum = -1;

    Packet(byte tipo, int numSeq, int numAck, byte[] dados) {
        this.tipo   = tipo;
        this.numSeq = numSeq;
        this.numAck = numAck;
        this.dados  = (dados != null) ? dados : new byte[0];
    }

    static Packet dataPacket(int numSeq, byte[] dados) {
        return new Packet(TYPE_DATA, numSeq, 0, dados);
    }

    static Packet ackPacket(int numAck) {
        return new Packet(TYPE_ACK, 0, numAck, null);
    }

    // Payload: float lossProb(4) + long fileSize(8) + int windowSize(4) + UTF-8 destPath
    static Packet handshakePacket(float lossProb, long fileSize, int windowSize, String destPath) {
        byte[] pathBytes = destPath.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buf = ByteBuffer.allocate(4 + 8 + 4 + pathBytes.length);
        buf.putFloat(lossProb);
        buf.putLong(fileSize);
        buf.putInt(windowSize);
        buf.put(pathBytes);
        return new Packet(TYPE_HANDSHAKE, 0, 0, buf.array());
    }

    static Packet finPacket(int numSeq, byte[] md5) {
        return new Packet(TYPE_FIN, numSeq, 0, md5 != null ? md5 : new byte[0]);
    }

    // Checksum cobre todos os campos do cabeçalho + payload
    private static int checksum(byte tipo, int numSeq, int numAck, byte[] dados) {
        int sum = (tipo & 0xFF);
        sum += ((numSeq >> 24) & 0xFF) + ((numSeq >> 16) & 0xFF)
             + ((numSeq >>  8) & 0xFF) +  (numSeq        & 0xFF);
        sum += ((numAck >> 24) & 0xFF) + ((numAck >> 16) & 0xFF)
             + ((numAck >>  8) & 0xFF) +  (numAck        & 0xFF);
        for (byte b : dados) sum += (b & 0xFF);
        return sum;
    }

    static byte[] serialize(Packet p) {
        ByteBuffer buf = ByteBuffer.allocate(HEADER_SIZE + p.dados.length);
        buf.put(p.tipo);
        buf.putInt(p.numSeq);
        buf.putInt(p.numAck);
        buf.putShort((short) p.dados.length);
        buf.putInt(checksum(p.tipo, p.numSeq, p.numAck, p.dados));
        buf.put(p.dados);
        return buf.array();
    }

    static Packet deserialize(byte[] raw, int len) {
        if (len < HEADER_SIZE) return null;
        ByteBuffer buf = ByteBuffer.wrap(raw, 0, len);
        byte  tipo    = buf.get();
        int   numSeq  = buf.getInt();
        int   numAck  = buf.getInt();
        short dataLen = buf.getShort();
        int   cs      = buf.getInt();
        if (dataLen < 0 || dataLen > MAX_DATA || len < HEADER_SIZE + dataLen) return null;
        byte[] dados = new byte[dataLen];
        buf.get(dados);
        Packet p = new Packet(tipo, numSeq, numAck, dados);
        p.storedChecksum = cs;
        return p;
    }

    boolean isCorrupt() {
        return storedChecksum != -1 && checksum(tipo, numSeq, numAck, dados) != storedChecksum;
    }

    static HandshakeInfo parseHandshake(byte[] dados) {
        ByteBuffer buf  = ByteBuffer.wrap(dados);
        float  loss     = buf.getFloat();
        long   fileSize = buf.getLong();
        int    winSize  = buf.getInt();
        byte[] pathBytes = new byte[dados.length - 16];
        buf.get(pathBytes);
        String destPath = new String(pathBytes, StandardCharsets.UTF_8);
        return new HandshakeInfo(loss, fileSize, winSize, destPath);
    }

    static class HandshakeInfo {
        final float  lossProb;
        final long   fileSize;
        final int    windowSize;
        final String destPath;

        HandshakeInfo(float lossProb, long fileSize, int windowSize, String destPath) {
            this.lossProb   = lossProb;
            this.fileSize   = fileSize;
            this.windowSize = windowSize;
            this.destPath   = destPath;
        }
    }
}
