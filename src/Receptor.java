import java.io.*;
import java.net.*;
import java.security.*;

public class Receptor {
    private static final int TIMEOUT_DUPLICATE_FIN_MS = 1000;

    private final DatagramSocket socket;
    private int   maxSeqNum;

    Receptor(int port) throws SocketException {
        this.socket = new DatagramSocket(port);
        System.out.printf("[RECEP] Aguardando handshake na porta %d...%n", port);
    }

    public static void main(String[] args) throws Exception {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : 5000;
        new Receptor(port).receive();
    }

    void receive() throws Exception {
        byte[] buf = new byte[Packet.MAX_PKT];

        // ---- Fase 1: Handshake ----
        DatagramPacket dp = new DatagramPacket(buf, buf.length);
        socket.receive(dp);

        Packet hsPkt = Packet.deserialize(dp.getData(), dp.getLength());
        if (hsPkt == null || hsPkt.isCorrupt() || hsPkt.tipo != Packet.TYPE_HANDSHAKE) {
            System.err.println("[ERRO] Primeiro pacote não é HANDSHAKE válido. Encerrando.");
            socket.close();
            return;
        }

        Packet.HandshakeInfo info = Packet.parseHandshake(hsPkt.dados);
        float  lossProb   = info.lossProb;
        long   fileSize   = info.fileSize;
        int    windowSize = info.windowSize;
        String destPath   = info.destPath;
        this.maxSeqNum    = windowSize * 2;

        System.out.printf("[HAND] Handshake recebido.%n");
        System.out.printf("[HAND] Destino: %s%n", destPath);
        System.out.printf("[HAND] Tamanho esperado: %,d bytes%n", fileSize);
        System.out.printf("[HAND] Janela: %d, maxSeqNum: %d, prob_perda: %.0f%%%n",
                windowSize, maxSeqNum, lossProb * 100);

        InetAddress emissorAddr = dp.getAddress();
        int         emissorPort = dp.getPort();

        // ACK do handshake
        sendAck(0, emissorAddr, emissorPort);
        System.out.println("[HAND] ACK enviado. Aguardando dados GBN...");

        // ---- Fase 2: GBN receive loop ----
        int     expectedSeqNum  = 0;
        boolean anyReceived     = false;
        int     acceptCount     = 0;   // pkts aceitos e gravados
        int     lossCount       = 0;   // pkts descartados por simulação de perda
        int     outOfOrderCount = 0;   // pkts descartados por estarem fora de ordem (GBN)
        byte[]  finMD5          = null;

        // Garante que o diretório pai do destPath existe
        File destFile = new File(destPath);
        if (destFile.getParentFile() != null) destFile.getParentFile().mkdirs();

        try (BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(destPath))) {
            while (true) {
                DatagramPacket incoming = new DatagramPacket(buf, buf.length);
                socket.receive(incoming);

                Packet pkt = Packet.deserialize(incoming.getData(), incoming.getLength());

                // Pacote corrompido
                if (pkt == null || pkt.isCorrupt()) {
                    System.out.println("[DISC ] Pacote corrompido — descartado");
                    if (anyReceived) {
                        sendAck((expectedSeqNum - 1 + maxSeqNum) % maxSeqNum, emissorAddr, emissorPort);
                    }
                    continue;
                }

                // FIN em ordem
                if (pkt.tipo == Packet.TYPE_FIN && pkt.numSeq == expectedSeqNum) {
                    System.out.printf("[FIN  ] Recebido seq=%d%n", pkt.numSeq);
                    bos.flush();
                    finMD5 = pkt.dados.length == 16 ? pkt.dados : null;
                    sendAck(pkt.numSeq, emissorAddr, emissorPort);
                    handleDuplicateFins(buf, emissorAddr, emissorPort);
                    break;
                }

                // DATA em ordem
                if (pkt.tipo == Packet.TYPE_DATA && pkt.numSeq == expectedSeqNum) {
                    // Simulação de perda (README §4): somente em pacotes recebidos em ordem
                    if (Math.random() < lossProb) {
                        lossCount++;
                        System.out.printf("[LOSS ] Perda simulada seq=%d  (total perdas=%d)%n",
                                pkt.numSeq, lossCount);
                        // Descarta silenciosamente, sem enviar ACK — forçará retransmissão
                    } else {
                        bos.write(pkt.dados);
                        acceptCount++;
                        anyReceived = true;
                        System.out.printf("[RECV ] seq=%-5d  (%d B)%n", pkt.numSeq, pkt.dados.length);
                        sendAck(expectedSeqNum, emissorAddr, emissorPort);
                        expectedSeqNum = (expectedSeqNum + 1) % maxSeqNum;
                    }
                    continue;
                }

                // DATA fora de ordem (ou FIN fora de ordem)
                if (pkt.tipo == Packet.TYPE_DATA) {
                    outOfOrderCount++;
                    System.out.printf("[DISC ] Fora de ordem: seq=%d (esperado=%d)%n",
                            pkt.numSeq, expectedSeqNum);
                    if (anyReceived) {
                        sendAck((expectedSeqNum - 1 + maxSeqNum) % maxSeqNum, emissorAddr, emissorPort);
                    }
                }
            }
        }

        socket.close();
        printStats(destPath, acceptCount, lossCount, outOfOrderCount, lossProb, finMD5);
    }

    // Reenvia ACK enquanto chegarem FINs duplicados (aguarda possíveis retransmissões do Emissor)
    private void handleDuplicateFins(byte[] buf, InetAddress addr, int port) {
        try {
            socket.setSoTimeout(TIMEOUT_DUPLICATE_FIN_MS);
            while (true) {
                DatagramPacket dp = new DatagramPacket(buf, buf.length);
                socket.receive(dp);
                Packet p = Packet.deserialize(dp.getData(), dp.getLength());
                if (p != null && p.tipo == Packet.TYPE_FIN) {
                    sendAck(p.numSeq, dp.getAddress(), dp.getPort());
                }
            }
        } catch (SocketTimeoutException ignored) {
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void sendAck(int numAck, InetAddress addr, int port) throws IOException {
        byte[] data = Packet.serialize(Packet.ackPacket(numAck));
        socket.send(new DatagramPacket(data, data.length, addr, port));
        System.out.printf("[ACK  ] Enviado numAck=%d%n", numAck);
    }

    private void printStats(String destPath, int acceptCount, int lossCount,
                            int outOfOrderCount, float lossProb, byte[] finMD5) throws Exception {
        int inOrderTotal = acceptCount + lossCount;
        double taxaEfetiva = inOrderTotal > 0 ? (100.0 * lossCount / inOrderTotal) : 0;

        System.out.println();
        System.out.println("=== Estatísticas do Receptor ===");
        System.out.printf("Arquivo salvo        : %s%n", destPath);
        System.out.printf("Pacotes aceitos      : %d%n", acceptCount);
        System.out.printf("Perdas simuladas     : %d%n", lossCount);
        System.out.printf("Fora de ordem (GBN)  : %d%n", outOfOrderCount);
        System.out.printf("Total em ordem       : %d%n", inOrderTotal);
        System.out.printf("Taxa de perda efet.  : %.2f%%  (configurada: %.0f%%)%n",
                taxaEfetiva, lossProb * 100);

        // Verificação de integridade MD5 (R9)
        if (finMD5 != null) {
            byte[] localMD5 = computeMD5(new File(destPath));
            String localHex = hexString(localMD5);
            String origHex  = hexString(finMD5);
            System.out.printf("MD5 recebido         : %s%n", localHex);
            if (localHex.equals(origHex)) {
                System.out.println("Integridade          : OK ✓  (MD5 confere)");
            } else {
                System.out.println("Integridade          : FALHA ✗  (MD5 diverge)");
                System.out.printf("  MD5 esperado: %s%n", origHex);
            }
        }
    }

    private static byte[] computeMD5(File file) throws Exception {
        MessageDigest md = MessageDigest.getInstance("MD5");
        try (FileInputStream fis = new FileInputStream(file)) {
            byte[] buf = new byte[8192];
            int n;
            while ((n = fis.read(buf)) != -1) md.update(buf, 0, n);
        }
        return md.digest();
    }

    private static String hexString(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) sb.append(String.format("%02x", b));
        return sb.toString();
    }
}
