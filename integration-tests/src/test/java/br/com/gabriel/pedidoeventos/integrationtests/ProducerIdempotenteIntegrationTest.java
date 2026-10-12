package br.com.gabriel.pedidoeventos.integrationtests;

import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import eu.rekawek.toxiproxy.model.toxic.Timeout;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.toxiproxy.ToxiproxyContainer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Fase 4 (Versão 2): prova, no broker, o efeito do producer idempotente.
 *
 * Um proxy (Toxiproxy) fica entre o producer e o Kafka. O teste engole as respostas do broker
 * (acks) enquanto envia uma mensagem: o broker grava a mensagem, o producer não recebe o ack,
 * estoura o timeout e reenvia. Sem idempotência o retry vira um registro duplicado no tópico;
 * com idempotência o broker reconhece o número de sequência repetido e descarta o retry.
 */
class ProducerIdempotenteIntegrationTest {

    private static final int PORTA_PROXY = 8666;
    private static final int PORTA_LISTENER_PROXY_NO_KAFKA = 9094;

    static Network rede = Network.newNetwork();
    static ToxiproxyContainer toxiproxy;
    static GenericContainer<?> kafka;
    static ToxiproxyClient toxiproxyClient;
    static Proxy proxyKafka;
    static String bootstrapServers;

    @BeforeAll
    static void subirAmbiente() throws Exception {
        toxiproxy = new ToxiproxyContainer("ghcr.io/shopify/toxiproxy:2.5.0")
                .withNetwork(rede)
                .withNetworkAliases("toxiproxy");
        toxiproxy.start();

        // O endereço que o Kafka anuncia aos clientes precisa ser o do proxy, senão o producer
        // contornaria o proxy depois do primeiro request de metadata.
        String enderecoDoProxyParaOCliente = toxiproxy.getHost() + ":" + toxiproxy.getMappedPort(PORTA_PROXY);

        kafka = new GenericContainer<>("apache/kafka:3.8.0")
                .withNetwork(rede)
                .withNetworkAliases("kafka")
                .withEnv("KAFKA_NODE_ID", "1")
                .withEnv("KAFKA_PROCESS_ROLES", "broker,controller")
                .withEnv("KAFKA_LISTENERS",
                        "PLAINTEXT://:9092,CONTROLLER://:9093,PROXY://:" + PORTA_LISTENER_PROXY_NO_KAFKA)
                .withEnv("KAFKA_ADVERTISED_LISTENERS",
                        "PLAINTEXT://kafka:9092,PROXY://" + enderecoDoProxyParaOCliente)
                .withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
                        "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PROXY:PLAINTEXT")
                .withEnv("KAFKA_INTER_BROKER_LISTENER_NAME", "PLAINTEXT")
                .withEnv("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
                .withEnv("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@kafka:9093")
                .withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
                .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
                .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
                .withEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
                .withEnv("CLUSTER_ID", "MkU3OEVBNTcwNTJENDM2Qk")
                .waitingFor(Wait.forLogMessage(".*Kafka Server started.*", 1)
                        .withStartupTimeout(Duration.ofMinutes(2)));
        kafka.start();

        toxiproxyClient = new ToxiproxyClient(toxiproxy.getHost(), toxiproxy.getControlPort());
        proxyKafka = toxiproxyClient.createProxy(
                "kafka", "0.0.0.0:" + PORTA_PROXY, "kafka:" + PORTA_LISTENER_PROXY_NO_KAFKA);
        bootstrapServers = enderecoDoProxyParaOCliente;
    }

    @AfterAll
    static void derrubarAmbiente() {
        if (kafka != null) kafka.stop();
        if (toxiproxy != null) toxiproxy.stop();
        rede.close();
    }

    @AfterEach
    void removerFalhasDeRede() throws Exception {
        toxiproxyClient.reset();
    }

    @Test
    void producerIdempotente_naoDuplicaNoBrokerQuandoOAckSePerde() throws Exception {
        long registros = publicarComAckPerdido(true, "teste-idempotente-" + UUID.randomUUID());

        System.out.println(">>> RESULTADO idempotente=true: registros no topico = " + registros);

        // 1 de aquecimento + 1 de teste. O retry foi deduplicado pelo broker.
        assertEquals(2, registros,
                "com enable.idempotence=true o retry não pode gerar registro duplicado no tópico");
    }

    @Test
    void controle_producerSemIdempotencia_duplicaNoBrokerQuandoOAckSePerde() throws Exception {
        long registros = publicarComAckPerdido(false, "teste-controle-" + UUID.randomUUID());

        System.out.println(">>> RESULTADO idempotente=false: registros no topico = " + registros);

        // Mesmo cenário sem idempotência: o retry é gravado de novo. Prova que a falha de rede
        // injetada realmente provoca duplicação, ou seja, que o teste acima é significativo.
        assertTrue(registros > 2,
                "sem idempotência o retry deveria duplicar a mensagem, mas o tópico tem " + registros + " registros");
    }

    /** Devolve quantos registros existem no tópico depois de um envio cujo ack foi perdido. */
    private long publicarComAckPerdido(boolean idempotente, String topico) throws Exception {
        try (KafkaProducer<String, String> producer = criarProducer(idempotente)) {
            // aquece: cria o tópico, busca metadata e (se for o caso) o producer id
            producer.send(new ProducerRecord<>(topico, "chave", "aquecimento")).get(20, TimeUnit.SECONDS);

            // engole as respostas do broker; o request do producer ainda chega e é gravado
            Timeout ackPerdido = proxyKafka.toxics().timeout("ack-perdido", ToxicDirection.DOWNSTREAM, 0);
            Future<RecordMetadata> envio = producer.send(new ProducerRecord<>(topico, "chave", "mensagem-de-teste"));

            // deixa o request estourar o request.timeout.ms (2s) e o producer reenviar uma vez
            Thread.sleep(2600);
            ackPerdido.remove();

            envio.get(20, TimeUnit.SECONDS);
        }
        return contarRegistros(topico);
    }

    private KafkaProducer<String, String> criarProducer(boolean idempotente) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, String.valueOf(idempotente));
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "2000");
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "30000");
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, "100");
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "15000");
        return new KafkaProducer<>(props);
    }

    /** O tópico é criado automaticamente com 1 partição, então registros = offset final - inicial. */
    private long contarRegistros(String topico) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "contador-" + UUID.randomUUID());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            TopicPartition particao = new TopicPartition(topico, 0);
            long inicio = consumer.beginningOffsets(List.of(particao)).get(particao);
            long fim = consumer.endOffsets(List.of(particao)).get(particao);
            return fim - inicio;
        }
    }
}
