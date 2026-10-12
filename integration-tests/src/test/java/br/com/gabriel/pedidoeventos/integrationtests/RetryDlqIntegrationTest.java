package br.com.gabriel.pedidoeventos.integrationtests;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.ComposeContainer;
import org.testcontainers.containers.ContainerState;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.File;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Fase 7 (Versão 5): retry com backoff exponencial e dead-letter queue.
 *
 * Um pedido com o produto "envenenado" falha sempre no payment-service. O teste confirma que ele
 * é tentado de novo, esgota as tentativas e vai para pedidos.criados.dlq, e que os demais pedidos
 * concluem. Como o retry é bloqueante e o listener tem uma única thread para as 3 partições, o
 * backoff do envenenado atrasa os pedidos que o consumer ainda não leu, de qualquer partição; o
 * teste mede as latências de uma rodada de referência (sem envenenado) e de uma rodada logo
 * depois do envenenado, e imprime a comparação.
 */
class RetryDlqIntegrationTest {

    static final String PRODUTO_ENVENENADO = "99999999-9999-9999-9999-999999999999";
    static final int PEDIDOS_POR_RODADA = 12;

    static ComposeContainer ambiente = new ComposeContainer(new File("../docker-compose.yml"))
            .withBuild(true)
            .withEnv("PRODUTO_ENVENENADO", PRODUTO_ENVENENADO)
            .withExposedService("order-service", 8080, Wait.forListeningPort().withStartupTimeout(Duration.ofMinutes(3)))
            .withExposedService("postgres", 5432, Wait.forListeningPort());

    static String orderServiceUrl;
    static String postgresUrl;

    @BeforeAll
    static void subirAmbiente() {
        ambiente.start();
        orderServiceUrl = "http://" + ambiente.getServiceHost("order-service", 8080)
                + ":" + ambiente.getServicePort("order-service", 8080);
        postgresUrl = "jdbc:postgresql://" + ambiente.getServiceHost("postgres", 5432)
                + ":" + ambiente.getServicePort("postgres", 5432) + "/tcc_pedidos";
    }

    @AfterAll
    static void derrubarAmbiente() {
        ambiente.stop();
    }

    @Test
    void poisonMessage_vaiParaDlqSemBloquearOsDemaisPedidos() throws Exception {
        aguardar("consumers prontos (partições atribuídas)", Duration.ofSeconds(120), () ->
                ocorrencias("payment-service", "partitions assigned") >= 1
                        && ocorrencias("stock-service", "partitions assigned") >= 1
                        && ocorrencias("order-service", "partitions assigned") >= 2);

        UUID produtoNormal = UUID.randomUUID();
        executar("INSERT INTO estoque.estoque (id, produto_id, nome_produto, quantidade_disponivel) "
                + "VALUES (gen_random_uuid(), '" + produtoNormal + "', 'Produto Normal', 1000)");

        // rodada de referência: sem pedido envenenado
        long[] referencia = enviarEMedir(produtoNormal.toString());

        // rodada com o envenenado enviado logo antes dos pedidos normais
        String pedidoEnvenenado = criarPedido(PRODUTO_ENVENENADO);
        long[] comVeneno = enviarEMedir(produtoNormal.toString());

        aguardar("tentativas esgotadas e mensagem na DLQ", Duration.ofSeconds(60), () ->
                ocorrencias("payment-service", "Tentativas esgotadas") >= 1);

        // 1 entrega original + 3 retentativas
        assertEquals(4, ocorrencias("payment-service", "falhou para pedidos.criados"),
                "o pedido envenenado deveria ter sido tentado 4 vezes");
        assertEquals(1, ocorrencias("payment-service", "Tentativas esgotadas"));
        assertEquals(1, offsetFinal("pedidos.criados.dlq"), "a DLQ deveria ter exatamente a mensagem envenenada");

        // o envenenado não deixou efeito nenhum: sem pagamento, sem evento na outbox, ainda PENDENTE
        assertEquals("PENDENTE", statusDoPedido(pedidoEnvenenado));
        assertEquals(0, contar("SELECT COUNT(*) FROM pagamento.pagamento WHERE pedido_id = '" + pedidoEnvenenado + "'"));
        assertEquals(0, contar("SELECT COUNT(*) FROM pagamento.outbox_events WHERE payload LIKE '%" + pedidoEnvenenado + "%'"));

        // os demais pedidos concluíram (enviarEMedir falha se algum não concluir) e cada um tem 1 pagamento
        assertEquals(2L * PEDIDOS_POR_RODADA, contar("SELECT COUNT(*) FROM pagamento.pagamento"),
                "cada pedido normal deve ter exatamente 1 pagamento");

        long limiteAtraso = mediana(referencia) + 2000;
        long atrasados = Arrays.stream(comVeneno).filter(ms -> ms > limiteAtraso).count();
        System.out.printf("[METRICA] referencia (sem veneno): mediana=%d ms, max=%d ms%n", mediana(referencia), maximo(referencia));
        System.out.printf("[METRICA] com veneno na frente:   mediana=%d ms, max=%d ms%n", mediana(comVeneno), maximo(comVeneno));
        System.out.printf("[METRICA] pedidos atrasados (> mediana de referencia + 2000 ms): %d de %d%n", atrasados, PEDIDOS_POR_RODADA);
    }

    /** Cria os pedidos normais e mede, para cada um, o tempo até o status CONCLUIDO. */
    private long[] enviarEMedir(String produtoId) throws Exception {
        String[] ids = new String[PEDIDOS_POR_RODADA];
        long[] inicio = new long[PEDIDOS_POR_RODADA];
        long[] latencia = new long[PEDIDOS_POR_RODADA];
        for (int i = 0; i < PEDIDOS_POR_RODADA; i++) {
            inicio[i] = System.nanoTime();
            ids[i] = criarPedido(produtoId);
        }
        List<Integer> pendentes = new ArrayList<>();
        for (int i = 0; i < PEDIDOS_POR_RODADA; i++) {
            pendentes.add(i);
        }
        long fim = System.nanoTime() + Duration.ofSeconds(90).toNanos();
        while (!pendentes.isEmpty() && System.nanoTime() < fim) {
            pendentes.removeIf(i -> {
                if ("CONCLUIDO".equals(statusDoPedido(ids[i]))) {
                    latencia[i] = (System.nanoTime() - inicio[i]) / 1_000_000;
                    return true;
                }
                return false;
            });
            Thread.sleep(100);
        }
        if (!pendentes.isEmpty()) {
            imprimirDiagnostico();
        }
        assertTrue(pendentes.isEmpty(), "pedidos normais não concluíram: " + pendentes.size());
        return latencia;
    }

    private static long mediana(long[] valores) {
        long[] ordenados = valores.clone();
        Arrays.sort(ordenados);
        return ordenados[ordenados.length / 2];
    }

    private static long maximo(long[] valores) {
        return Arrays.stream(valores).max().orElse(0);
    }

    // ---- operações sobre os containers ----

    private static ContainerState container(String servico) {
        return ambiente.getContainerByServiceName(servico)
                .orElseThrow(() -> new IllegalStateException("container não encontrado: " + servico));
    }

    private static long ocorrencias(String servico, String trecho) {
        String logs = container(servico).getLogs();
        long total = 0;
        int indice = logs.indexOf(trecho);
        while (indice >= 0) {
            total++;
            indice = logs.indexOf(trecho, indice + trecho.length());
        }
        return total;
    }

    /** Offset final da partição 0 do tópico (a DLQ tem 1 partição) = quantidade de mensagens nela. */
    private static long offsetFinal(String topico) throws Exception {
        String saida = container("kafka").execInContainer(
                "/opt/kafka/bin/kafka-get-offsets.sh",
                "--bootstrap-server", "localhost:9092",
                "--topic", topico).getStdout().trim();
        // formato: topico:particao:offset
        return Long.parseLong(saida.substring(saida.lastIndexOf(':') + 1));
    }

    // ---- acesso ao sistema e ao banco ----

    private String criarPedido(String produtoId) throws Exception {
        String corpo = """
                {"itens":[{"produtoId":"%s","quantidade":1,"precoUnitario":50.00}]}
                """.formatted(produtoId);
        HttpResponse<String> resposta = HttpClient.newHttpClient().send(
                HttpRequest.newBuilder(URI.create(orderServiceUrl + "/pedidos"))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(corpo))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        assertEquals(202, resposta.statusCode());
        return resposta.body().split("\"id\":\"")[1].split("\"")[0];
    }

    private String statusDoPedido(String pedidoId) {
        try {
            HttpResponse<String> resposta = HttpClient.newHttpClient().send(
                    HttpRequest.newBuilder(URI.create(orderServiceUrl + "/pedidos/" + pedidoId)).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            return resposta.body().split("\"status\":\"")[1].split("\"")[0];
        } catch (Exception e) {
            return "INDISPONIVEL";
        }
    }

    private static void executar(String sql) throws Exception {
        try (Connection conn = DriverManager.getConnection(postgresUrl, "tcc", "tcc");
             Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        }
    }

    private static long contar(String sql) {
        try (Connection conn = DriverManager.getConnection(postgresUrl, "tcc", "tcc");
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            rs.next();
            return rs.getLong(1);
        } catch (Exception e) {
            return -1;
        }
    }

    private static void aguardar(String descricao, Duration limite, Supplier<Boolean> condicao) throws Exception {
        long fim = System.nanoTime() + limite.toNanos();
        while (System.nanoTime() < fim) {
            if (condicao.get()) {
                return;
            }
            Thread.sleep(500);
        }
        imprimirDiagnostico();
        assertTrue(false, "tempo esgotado esperando: " + descricao);
    }

    /** Quando uma espera estoura, mostra o fim dos logs de cada serviço para facilitar a investigação. */
    private static void imprimirDiagnostico() {
        for (String servico : List.of("order-service", "payment-service", "stock-service")) {
            try {
                String[] linhas = container(servico).getLogs().split("\n");
                int inicio = Math.max(0, linhas.length - 40);
                System.err.println("===== DIAGNOSTICO " + servico + " (últimas linhas) =====");
                for (int i = inicio; i < linhas.length; i++) {
                    System.err.println(linhas[i]);
                }
            } catch (Exception e) {
                System.err.println("não foi possível ler os logs de " + servico + ": " + e);
            }
        }
    }
}
