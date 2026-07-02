import java.io.*;
import java.net.*;
import java.security.*;
import java.util.*;

/**
 * Lado emissor do protocolo Go-Back-N sobre UDP.
 *
 * Ideia central do GBN: existe uma "janela" de até `windowSize` pacotes que podem
 * estar em voo (enviados, mas ainda não confirmados) ao mesmo tempo. `base` é o mais
 * antigo pacote não confirmado; `nextSeqNum` é o próximo número de sequência livre.
 * Um único timer cobre o pacote mais antigo da janela (base): se ele estourar,
 * a JANELA INTEIRA é retransmitida (diferente de Selective Repeat, que reenviaria
 * só o pacote perdido). ACKs são cumulativos: um ACK(n) confirma tudo até n.
 */
public class Emissor {
    private static final int   TIMEOUT_MS     = 500; // tempo de espera antes de retransmitir
    private static final int   MAX_RETRIES    = 10;  // tentativas no handshake/FIN (stop-and-wait)
    private static final int   PROGRESS_EVERY = 50;  // imprime progresso a cada N pacotes

    private final String         srcFile;
    private final String         destPath;
    private final InetAddress    receptorAddr;
    private final int            receptorPort;
    private final int            windowSize;
    private final int            maxSeqNum;   // = windowSize * 2 (evita ambiguidade de seq wrap-around)
    private final float          lossProb;    // só informativo aqui; a perda é simulada no Receptor
    private final DatagramSocket socket;

    // ---- Estado do GBN (protegido pelo monitor 'this', pois é acessado por 2 threads) ----
    private int      base       = 0; // seq do pacote mais antigo ainda não confirmado
    private int      nextSeqNum = 0; // próximo seq a ser usado no envio
    private Packet[] window;         // buffer circular com os pacotes atualmente em voo
    private final Timer timer = new Timer(true); // timer único, compartilhado pela janela
    private TimerTask   currentTask;

    // Contadores de estatísticas (só para o relatório final)
    private int  totalEnviados  = 0;
    private int  retransmissoes = 0;
    private int  acksRecebidos  = 0;
    private long startTime;

    private volatile boolean ackThreadRunning = true; // flag para encerrar a thread de ACKs

    Emissor(String srcFile, String ipAndPath, float lossProb, int windowSize, int port)
            throws Exception {
        int colon         = ipAndPath.indexOf(':');
        this.receptorAddr = InetAddress.getByName(ipAndPath.substring(0, colon));
        this.destPath     = ipAndPath.substring(colon + 1);
        this.srcFile      = srcFile;
        this.lossProb     = lossProb;
        this.windowSize   = windowSize;
        this.maxSeqNum    = windowSize * 2;
        this.window       = new Packet[windowSize];
        this.receptorPort = port;
        this.socket       = new DatagramSocket();
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.out.println("Uso: java Emissor <arquivo_origem> <IP>:<path_destino> <tamanho_janela> [prob_perda] [porta]");
            System.out.println("Ex:  java Emissor foto.jpg 192.168.0.10:/tmp/foto.jpg 8 0.10");
            return;
        }
        String srcFile    = args[0];
        String ipAndPath  = args[1];
        int    windowSize = Integer.parseInt(args[2]);
        float  lossProb   = args.length > 3 ? Float.parseFloat(args[3].replace(',', '.')) : 0.10f;
        int    port       = args.length > 4 ? Integer.parseInt(args[4]) : 5000;
        new Emissor(srcFile, ipAndPath, lossProb, windowSize, port).transfer();
    }

    // Orquestra a transferência completa: handshake -> envio GBN -> FIN -> estatísticas
    void transfer() throws Exception {
        File file = new File(srcFile);
        if (!file.exists()) {
            System.err.println("[ERRO] Arquivo não encontrado: " + srcFile);
            return;
        }
        long   fileSize = file.length();
        byte[] md5      = computeMD5(file); // hash do arquivo original, usado depois para checar integridade

        System.out.printf("[EMIS] Arquivo: %s (%,d bytes)%n", srcFile, fileSize);
        System.out.printf("[EMIS] Destino: %s:%s%n", receptorAddr.getHostAddress(), destPath);
        System.out.printf("[EMIS] Janela=%d, maxSeqNum=%d, perda configurada=%.0f%%  (simulada no Receptor)%n",
                windowSize, maxSeqNum, lossProb * 100);

        // 1. Handshake (stop-and-wait simples, fora do fluxo GBN)
        sendHandshake(lossProb, fileSize, windowSize, destPath);
        System.out.println("[HAND] Handshake confirmado. Iniciando transferência GBN...");

        // 2. GBN data transfer
        startTime = System.currentTimeMillis();
        // Thread separada só para escutar ACKs, rodando em paralelo ao laço de envio abaixo.
        // É isso que permite "enviar vários pacotes seguidos" sem bloquear esperando cada ACK.
        Thread ackThread = new Thread(this::receiveAcks, "ack-thread");
        ackThread.setDaemon(true);
        ackThread.start();

        int totalDataPkts = 0;
        try (FileInputStream fis = new FileInputStream(file)) {
            byte[] buf = new byte[Packet.MAX_DATA];
            int n;
            // Lê o arquivo em blocos de até 1024 bytes e entrega cada bloco ao GBN
            while ((n = fis.read(buf)) != -1) {
                rdtSend(Arrays.copyOf(buf, n)); // bloqueia aqui se a janela estiver cheia
                totalDataPkts++;
                if (totalDataPkts % PROGRESS_EVERY == 0) printProgress(fileSize);
            }
        }

        // Aguardar confirmação de todos os pacotes: base só alcança nextSeqNum
        // quando o último ACK da janela chegar (onAck faz o notifyAll()).
        synchronized (this) {
            while (base != nextSeqNum) wait();
            stopTimer();
        }
        ackThreadRunning = false;
        ackThread.interrupt();
        ackThread.join(1000);

        // 3. FIN (stop-and-wait, carrega o MD5 para o Receptor validar)
        sendFin(nextSeqNum, md5);
        socket.close();

        printStats(srcFile, fileSize, totalDataPkts, md5);
    }

    // ---- Handshake ----

    // Envia o pacote HANDSHAKE e espera o ACK(0) do Receptor, retransmitindo em caso de timeout.
    // Padrão stop-and-wait: só existe 1 pacote em voo aqui, diferente do envio GBN.
    private void sendHandshake(float lossProb, long fileSize, int windowSize, String destPath)
            throws Exception {
        Packet pkt    = Packet.handshakePacket(lossProb, fileSize, windowSize, destPath);
        byte[] bytes  = Packet.serialize(pkt);
        byte[] ackBuf = new byte[Packet.HEADER_SIZE];

        socket.setSoTimeout(TIMEOUT_MS);
        for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
            socket.send(new DatagramPacket(bytes, bytes.length, receptorAddr, receptorPort));
            System.out.printf("[HAND] Handshake enviado (tentativa %d)%n", attempt);
            try {
                DatagramPacket dp = new DatagramPacket(ackBuf, ackBuf.length);
                socket.receive(dp);
                Packet ack = Packet.deserialize(dp.getData(), dp.getLength());
                if (ack != null && !ack.isCorrupt()
                        && ack.tipo == Packet.TYPE_ACK && ack.numAck == 0) return;
            } catch (SocketTimeoutException e) {
                System.out.println("[HAND] timeout, retransmitindo...");
            }
        }
        throw new RuntimeException("Handshake não confirmado após " + MAX_RETRIES + " tentativas.");
    }

    // ---- FSM GBN (Emissor) ----

    // "Enviar" do GBN: chamado uma vez por bloco de arquivo.
    // Bloqueia (wait) enquanto a janela estiver cheia; onAck() acorda esta thread via notifyAll().
    private synchronized void rdtSend(byte[] dados) throws InterruptedException {
        // Bloqueia enquanto a janela estiver cheia
        while (seqDiff(nextSeqNum, base) >= windowSize) wait();

        Packet pkt = Packet.dataPacket(nextSeqNum, dados);
        window[nextSeqNum % windowSize] = pkt; // guarda para poder retransmitir depois
        if (base == nextSeqNum) startTimer(); // inicia timer no primeiro pkt da janela
        udpSend(pkt);
        System.out.printf("[SEND] seq=%-5d  (%d B)%n", nextSeqNum, dados.length);
        totalEnviados++;
        nextSeqNum = (nextSeqNum + 1) % maxSeqNum;
    }

    // Loop da thread dedicada a ACKs: fica recebendo continuamente até ackThreadRunning virar false.
    // Timeout curto (100ms) só para poder checar a flag de parada periodicamente.
    private void receiveAcks() {
        byte[] buf = new byte[Packet.HEADER_SIZE];
        try { socket.setSoTimeout(100); } catch (SocketException ignored) {}
        while (ackThreadRunning) {
            try {
                DatagramPacket dp = new DatagramPacket(buf, buf.length);
                socket.receive(dp);
                Packet ack = Packet.deserialize(dp.getData(), dp.getLength());
                if (ack == null || ack.isCorrupt() || ack.tipo != Packet.TYPE_ACK) continue;
                onAck(ack.numAck);
            } catch (SocketTimeoutException ignored) {
            } catch (Exception e) {
                if (ackThreadRunning) e.printStackTrace();
            }
        }
    }

    // Processa um ACK recebido. ACKs são CUMULATIVOS: ACK(n) confirma tudo até n, inclusive.
    private synchronized void onAck(int n) {
        int inFlight = seqDiff(nextSeqNum, base); // quantos pacotes estão em voo agora
        int ackDist  = seqDiff(n, base);
        // Ignora ACKs fora da janela em voo (atrasados ou duplicados) — protege contra
        // reordenação/duplicação na rede confundir o estado da janela.
        if (inFlight == 0 || ackDist >= inFlight) return;

        System.out.printf("[ACK ] n=%-5d  (base=%d → %d)%n", n, base, (n + 1) % maxSeqNum);
        acksRecebidos++;
        base = (n + 1) % maxSeqNum; // avança a janela
        if (base == nextSeqNum) stopTimer();  // janela vazia: nada mais para vigiar
        else startTimer();                    // reinicia o timer para o novo pacote mais antigo
        notifyAll(); // acorda rdtSend() caso esteja esperando espaço na janela
    }

    // Timeout do pacote mais antigo da janela: retransmite TODOS os pacotes ainda em voo
    // (de base até nextSeqNum-1). Essa é a essência do "Go-Back-N": não seleciona
    // individualmente o que se perdeu, simplesmente reenvia a janela inteira.
    private synchronized void onTimeout() {
        int count = seqDiff(nextSeqNum, base);
        System.out.printf("[TIMEO] Retransmitindo %d pacote(s), base=%d%n", count, base);
        startTimer();
        for (int i = 0; i < count; i++) {
            int seq = (base + i) % maxSeqNum;
            udpSend(window[seq % windowSize]);
            System.out.printf("[RETX ] seq=%d%n", seq);
            totalEnviados++;
            retransmissoes++;
        }
    }

    // Reagenda o timer único a partir de agora (usado tanto ao iniciar quanto ao retransmitir)
    private void startTimer() {
        stopTimer();
        currentTask = new TimerTask() { public void run() { onTimeout(); } };
        timer.schedule(currentTask, TIMEOUT_MS);
    }

    private void stopTimer() {
        if (currentTask != null) { currentTask.cancel(); currentTask = null; }
    }

    private void udpSend(Packet pkt) {
        try {
            byte[] data = Packet.serialize(pkt);
            socket.send(new DatagramPacket(data, data.length, receptorAddr, receptorPort));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // ---- FIN ----

    // Handshake de encerramento: envia FIN (com MD5 do arquivo) e espera o FIN-ACK correspondente.
    // Mesmo padrão stop-and-wait do handshake inicial.
    private void sendFin(int finSeq, byte[] md5) throws Exception {
        Packet pkt    = Packet.finPacket(finSeq, md5);
        byte[] bytes  = Packet.serialize(pkt);
        byte[] ackBuf = new byte[Packet.HEADER_SIZE];

        socket.setSoTimeout(TIMEOUT_MS);
        for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
            socket.send(new DatagramPacket(bytes, bytes.length, receptorAddr, receptorPort));
            System.out.printf("[FIN  ] enviado seq=%d (tentativa %d)%n", finSeq, attempt);
            try {
                DatagramPacket dp = new DatagramPacket(ackBuf, ackBuf.length);
                socket.receive(dp);
                Packet ack = Packet.deserialize(dp.getData(), dp.getLength());
                if (ack != null && !ack.isCorrupt()
                        && ack.tipo == Packet.TYPE_ACK && ack.numAck == finSeq) {
                    System.out.println("[FIN-A] FIN-ACK recebido. Encerramento confirmado.");
                    return;
                }
            } catch (SocketTimeoutException e) {
                System.out.println("[FIN  ] timeout, retransmitindo...");
            }
        }
        System.out.println("[FIN  ] Aviso: FIN-ACK não recebido. Encerrando mesmo assim.");
    }

    // ---- Estatísticas e utilitários ----

    private void printProgress(long fileSize) {
        long   elapsed = System.currentTimeMillis() - startTime;
        double secs    = elapsed / 1000.0;
        double tput    = secs > 0 ? (acksRecebidos * (double) Packet.MAX_DATA * 8 / 1e6) / secs : 0;
        System.out.printf("[PROG ] enviados=%d  acks=%d  retx=%d  throughput=%.2f Mbit/s%n",
                totalEnviados, acksRecebidos, retransmissoes, tput);
    }

    private void printStats(String srcFile, long fileSize, int totalDataPkts, byte[] md5) {
        long   elapsed = System.currentTimeMillis() - startTime;
        double secs    = elapsed / 1000.0;
        double tput    = secs > 0 ? (fileSize * 8.0 / 1e6) / secs : 0;
        System.out.println();
        System.out.println("=== Estatísticas do Emissor ===");
        System.out.printf("Arquivo          : %s (%,d bytes)%n", srcFile, fileSize);
        System.out.printf("Pacotes de dados : %d%n", totalDataPkts);
        System.out.printf("Total enviados   : %d  (incl. retransmissões)%n", totalEnviados);
        System.out.printf("Retransmissões   : %d%n", retransmissoes);
        System.out.printf("ACKs recebidos   : %d%n", acksRecebidos);
        System.out.printf("Tempo total      : %.2f s%n", secs);
        System.out.printf("Throughput       : %.2f Mbit/s%n", tput);
        System.out.printf("MD5 origem       : %s%n", hexString(md5));
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

    static String hexString(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) sb.append(String.format("%02x", b));
        return sb.toString();
    }

    // Distância circular de 'from' até 'n' no espaço de sequência [0, maxSeqNum).
    // Necessário porque numSeq dá "wrap-around" (volta a 0 depois de maxSeqNum-1).
    private int seqDiff(int n, int from) {
        return (n - from + maxSeqNum) % maxSeqNum;
    }
}
