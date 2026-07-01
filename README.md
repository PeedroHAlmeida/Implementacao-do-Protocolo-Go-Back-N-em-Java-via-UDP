# Trabalho de Redes - Protocolo Go-Back-N (GBN) sobre UDP

Implementação dos módulos Emissor e Receptor em Java, comunicando-se via sockets UDP e simulando transferência confiável de arquivos com o protocolo Go-Back-N.

O enunciado completo do trabalho está em [ENUNCIADO.md](ENUNCIADO.md)

## Estrutura do projeto

```text
trabRedes/
├── src/                  # Código-fonte Java
│   ├── Emissor.java
│   ├── Packet.java
│   └── Receptor.java
├── testes/               # Arquivos usados para validar a transferência
│   ├── entrada/          # Arquivos originais (lado do Emissor)
│   └── saida/            # Arquivos recebidos pelo Receptor (devem ser idênticos aos de entrada)
├── ENUNCIADO.md          # Especificação original do trabalho
└── README.md             # Este arquivo
```

## Código-fonte Java

Os arquivos-fonte do projeto estão organizados no diretório `src/` e contêm
a implementação completa do protocolo Go-Back-N sobre UDP.

## Como compilar

**Importante:** execute todos os comandos abaixo a partir da raiz do projeto 
(a pasta `trabRedes/`, que contém `src/` e `testes/`).

```bash
javac -d out src/*.java
```
Isso compila os arquivos `.java` de `src/` e coloca os `.class` em `out/` (pasta 
separada, sem misturar com o código-fonte).

## Como executar
Abra dois terminais, ambos com o diretório atual na raiz do projeto.

### Terminal 1 - Inicie o Receptor
O Receptor deve ser iniciado primeiro, indicando a porta UDP (exemplo: 5000):

```bash
java -cp out Receptor 5000
```

### Terminal 2 - Inicie o Emissor
O Receptor deve ser iniciado primeiro, indicando a porta UDP (exemplo: 5000):

```bash
java -cp out Emissor <arquivo_origem> <IP_destino>:<path_destino> <tamanho_janela> <prob_perda>
```
### Parâmetros:

`<arquivo_origem>`: caminho para o arquivo que será enviado

`<IP_destino>:<path_destino>`: endereço IP do receptor e caminho onde o arquivo será salvo

`<tamanho_janela>`: tamanho da janela deslizante (em número de pacotes)

`<prob_perda>`: probabilidade de perda de pacotes (valor entre 0 e 1)

Exemplo para executar:
```bash
java -cp out Emissor testes/entrada/pdf_teste.pdf 127.0.0.1:testes/saida/pdf_teste.pdf 8 0.10
```
## Saída dos Programas

Ao final da transferência:

### Emissor

| Campo            | O que significa                                                                         |
|:-----------------|-----------------------------------------------------------------------------------------|
| Arquivo          | Nome e tamanho do arquivo original                                                      | 
| Pacotes de dados | Quantos pacotes foram necessários para cobrir o arquivo (sem contar retransmissões)     |
| Total enviados   | Pacotes de dados + retransmissões — o que realmente saiu pelo socke                     |
| Retransmissões   | Quantas vezes o GBN teve que reenviar por timeout (janela inteira reenviada a cada vez) |
| ACKs recebidos   | Confirmações válidas recebidas do Receptor                                              |
| Tempo total      | Do início da transferência até o FIN-ACK                                                |
| Throughput       | Taxa efetiva: (tamanho do arquivo × 8) / tempo, em Mbit/s                               |
| MD5 origem       | Hash do arquivo original, calculado antes de enviar                                     |


### Receptor

| Campo                 | O que significa                                                                                                                                                                                |
|:----------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Arquivo salvo         | Caminho onde o arquivo foi gravado                                                                                                                                                             |
| Pacotes aceitos       | Pacotes que chegaram em ordem e foram gravados no disco                                                                                                                                        |
| Perdas simuladas      | Pacotes que chegaram em ordem mas foram descartados intencionalmente pelo Receptor para simular falha de rede (controlado pelo parâmetro prob_perda)                                           |
| Fora de ordem (GBN)   | Pacotes descartados por chegarem fora de ordem — consequência direta das retransmissões do GBN (o Emissor reenvia a janela toda, e os pacotes que o Receptor já tinha recebido chegam de novo) |
| Total em ordem        | Aceitos + Perdas simuladas — quantos chegaram no momento certo (independente de terem sido aceitos ou não)                                                                                     |
| Taxa de perda efetiva | Perdas / Total em ordem — a perda real que aconteceu, que pode diferir um pouco da configurada por ser aleatória                                                                               |
| MD5 recebido          | Hash do arquivo salvo no disco                                                                                                                                                                    |
| Integridade           | Compara o MD5 do arquivo recebido com o MD5 enviado pelo Emissor no pacote FIN — confirma se o arquivo chegou íntegro |


## Requisitos do Sistema

- Java 8 ou superior
- Conexão de rede (localhost ou rede local) para comunicação UDP

## Autores

- Pedro Henrique de Almeida
- Jorran Luka Andrade dos Santos

## Licença

Este projeto foi desenvolvido como trabalho acadêmico para a disciplina de Redes 
de Computadores ministrada por Prof. Dr. Flávio Barbieri Gonzaga.# Implementacao-do-Protocolo-Go-Back-N-em-Java-via-UDP
