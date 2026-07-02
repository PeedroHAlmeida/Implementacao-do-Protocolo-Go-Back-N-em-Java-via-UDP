import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Representa um pacote do protocolo (formato de rede + serialização).
 * Não tem lógica de GBN — só sabe montar/desmontar bytes e validar checksum.
 *
 * Layout no fio:
 *   tipo(1B) | numSeq(4B) | numAck(4B) | tamanhoDados(2B) | checksum(4B) | dados(0..1024B)
 */
public class Packet {
    // Tipos de pacote que trafegam entre Emissor e Receptor
    static final byte TYPE_DATA      = 0; // pacote com dados do arquivo
    static final byte TYPE_ACK       = 1; // confirmação (cumulativa) de recebimento
    static final byte TYPE_HANDSHAKE = 2; // primeiro pacote: negocia parâmetros da transferência
    static final byte TYPE_FIN       = 3; // sinaliza fim da transferência (carrega o MD5 do arquivo)

    static final int MAX_DATA    = 1024; // payload máximo de um pacote DATA, em bytes
    // tipo(1) + num_seq(4) + num_ack(4) + tamanho_dados(2) + checksum(4) = 15 bytes
    static final int HEADER_SIZE = 15;
    static final int MAX_PKT     = HEADER_SIZE + MAX_DATA; // tamanho máximo de um datagrama

    final byte   tipo;
    final int    numSeq;
    final int    numAck;
    final byte[] dados;

    // Checksum que veio no pacote recebido da rede (-1 = pacote foi criado localmente, não recebido)
    private int storedChecksum = -1;

    Packet(byte tipo, int numSeq, int numAck, byte[] dados) {
        this.tipo   = tipo;
        this.numSeq = numSeq;
        this.numAck = numAck;
        this.dados  = (dados != null) ? dados : new byte[0];
    }

    // Fábrica de pacote de dados: carrega o número de sequência e o pedaço do arquivo
    static Packet dataPacket(int numSeq, byte[] dados) {
        return new Packet(TYPE_DATA, numSeq, 0, dados);
    }

    // Fábrica de ACK: numAck é cumulativo (confirma tudo até esse número, inclusive)
    static Packet ackPacket(int numAck) {
        return new Packet(TYPE_ACK, 0, numAck, null);
    }

    // Payload: float lossProb(4) + long fileSize(8) + int windowSize(4) + UTF-8 destPath
    // O Emissor usa esse pacote para "avisar" o Receptor de tudo que ele precisa saber
    // antes de começar a receber dados (tamanho de janela, path de destino etc.)
    static Packet handshakePacket(float lossProb, long fileSize, int windowSize, String destPath) {
        byte[] pathBytes = destPath.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buf = ByteBuffer.allocate(4 + 8 + 4 + pathBytes.length);
        buf.putFloat(lossProb);
        buf.putLong(fileSize);
        buf.putInt(windowSize);
        buf.put(pathBytes);
        return new Packet(TYPE_HANDSHAKE, 0, 0, buf.array());
    }

    // Fábrica de FIN: numSeq é o próximo seq não usado (equivale a "enviei tudo até aqui");
    // carrega o MD5 do arquivo original para o Receptor validar integridade no final
    static Packet finPacket(int numSeq, byte[] md5) {
        return new Packet(TYPE_FIN, numSeq, 0, md5 != null ? md5 : new byte[0]);
    }

    // Checksum cobre todos os campos do cabeçalho + payload.
    // É uma soma simples byte a byte (não é CRC) — suficiente para detectar corrupção
    // simulada/aleatória neste trabalho, mas não é criptograficamente robusta.
    private static int checksum(byte tipo, int numSeq, int numAck, byte[] dados) {
        int sum = (tipo & 0xFF);
        sum += ((numSeq >> 24) & 0xFF) + ((numSeq >> 16) & 0xFF)
             + ((numSeq >>  8) & 0xFF) +  (numSeq        & 0xFF);
        sum += ((numAck >> 24) & 0xFF) + ((numAck >> 16) & 0xFF)
             + ((numAck >>  8) & 0xFF) +  (numAck        & 0xFF);
        for (byte b : dados) sum += (b & 0xFF);
        return sum;
    }

    // Converte o objeto Packet em bytes prontos para ir dentro de um DatagramPacket
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

    // Reconstrói um Packet a partir dos bytes recebidos via UDP.
    // Retorna null se o pacote estiver visivelmente malformado (tamanho inconsistente),
    // o que é diferente de "corrompido" (isso é checado depois, via isCorrupt()).
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
        p.storedChecksum = cs; // guarda o checksum recebido para comparar em isCorrupt()
        return p;
    }

    // Recalcula o checksum local e compara com o que veio no pacote.
    // Se storedChecksum == -1, o pacote foi criado localmente (nunca passou pela rede),
    // então não há o que validar.
    boolean isCorrupt() {
        return storedChecksum != -1 && checksum(tipo, numSeq, numAck, dados) != storedChecksum;
    }

    // Desmonta o payload do pacote HANDSHAKE de volta nos parâmetros da transferência
    static HandshakeInfo parseHandshake(byte[] dados) {
        ByteBuffer buf  = ByteBuffer.wrap(dados);
        float  loss     = buf.getFloat();
        long   fileSize = buf.getLong();
        int    winSize  = buf.getInt();
        byte[] pathBytes = new byte[dados.length - 16]; // 16 = 4(float) + 8(long) + 4(int)
        buf.get(pathBytes);
        String destPath = new String(pathBytes, StandardCharsets.UTF_8);
        return new HandshakeInfo(loss, fileSize, winSize, destPath);
    }

    // Struct simples para devolver os campos decodificados do handshake
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
