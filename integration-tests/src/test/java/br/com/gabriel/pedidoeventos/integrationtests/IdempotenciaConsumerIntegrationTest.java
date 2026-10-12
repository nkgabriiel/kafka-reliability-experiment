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
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Fase 5 (Versão 3): idempotência de consumer.
 *
 * Processa um pedido normalmente, depois zera os offsets dos consumer groups dos três serviços
 * (replay de todas as mensagens do tópico) e confirma que o efeito de negócio não se repete:
 * pagamento, estoque e status do pedido ficam iguais, e os consumers registram o descarte.
 */
class IdempotenciaConsumerIntegrationTest {

    static ComposeContainer ambiente = new ComposeContainer(new File("../docker-compose.yml"))
            .withBuild(true)
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
    void replayDasMensagens_naoDuplicaOEfeitoDeNegocio() throws Exception {
        // a porta do order-service abre antes de os consumers entrarem nos grupos
        aguardar("consumers prontos (partições atribuídas)", Duration.ofSeconds(120), () ->
                ocorrencias("payment-service", "partitions assigned") >= 1
                        && ocorrencias("stock-service", "partitions assigned") >= 1
                        && ocorrencias("order-service", "partitions assigned") >= 2);

        UUID produtoId = UUID.randomUUID();
        executar("INSERT INTO estoque.estoque (id, produto_id, nome_produto, quantidade_disponivel) "
                + "VALUES (gen_random_uuid(), '" + produtoId + "', 'Produto Replay', 10)");

        String pedidoId = criarPedido(produtoId, 2);
        aguardar("pedido concluído", Duration.ofSeconds(60),
                () -> "CONCLUIDO".equals(statusDoPedido(pedidoId)));

        assertEquals(1, contar("SELECT COUNT(*) FROM pagamento.pagamento WHERE pedido_id = '" + pedidoId + "'"));
        assertEquals(8, contar("SELECT quantidade_disponivel FROM estoque.estoque WHERE produto_id = '" + produtoId + "'"));
        long eventosPedido = contar("SELECT COUNT(*) FROM pedido.eventos_processados");
        long eventosPagamento = contar("SELECT COUNT(*) FROM pagamento.eventos_processados");
        long eventosEstoque = contar("SELECT COUNT(*) FROM estoque.eventos_processados");

        // replay: para os consumers, volta os offsets de cada grupo para o início e sobe de novo
        List<String> servicos = List.of("order-service", "payment-service", "stock-service");
        for (String servico : servicos) {
            parar(servico);
        }
        for (String grupo : servicos) {
            resetarOffsets(grupo);
        }
        for (String servico : servicos) {
            iniciar(servico);
        }

        // cada consumer reentrega as mensagens do seu(s) tópico(s) e precisa descartá-las:
        // payment lê 1 evento, stock lê 1, order lê 2 (pagamentos.processados e estoque.processado)
        aguardar("descartes nos logs dos serviços", Duration.ofSeconds(120), () ->
                ocorrencias("payment-service", "descartando sem reprocessar") >= 1
                        && ocorrencias("stock-service", "descartando sem reprocessar") >= 1
                        && ocorrencias("order-service", "descartando sem reprocessar") >= 2);

        assertEquals(1, contar("SELECT COUNT(*) FROM pagamento.pagamento WHERE pedido_id = '" + pedidoId + "'"),
                "o replay não pode gerar um segundo pagamento");
        assertEquals(8, contar("SELECT quantidade_disponivel FROM estoque.estoque WHERE produto_id = '" + produtoId + "'"),
                "o replay não pode reservar o estoque de novo");
        assertEquals("CONCLUIDO", statusDoPedido(pedidoId));
        assertEquals(eventosPedido, contar("SELECT COUNT(*) FROM pedido.eventos_processados"));
        assertEquals(eventosPagamento, contar("SELECT COUNT(*) FROM pagamento.eventos_processados"));
        assertEquals(eventosEstoque, contar("SELECT COUNT(*) FROM estoque.eventos_processados"));
    }

    // ---- operações sobre os containers ----

    private static ContainerState container(String servico) {
        return ambiente.getContainerByServiceName(servico)
                .orElseThrow(() -> new IllegalStateException("container não encontrado: " + servico));
    }

    private static void parar(String servico) {
        ContainerState c = container(servico);
        c.getDockerClient().stopContainerCmd(c.getContainerId()).exec();
    }

    private static void iniciar(String servico) {
        ContainerState c = container(servico);
        c.getDockerClient().startContainerCmd(c.getContainerId()).exec();
    }

    /** O grupo só pode ser resetado depois de ficar vazio; tenta até o broker aceitar. */
    private static void resetarOffsets(String grupo) throws Exception {
        ContainerState kafka = container("kafka");
        aguardar("reset de offsets do grupo " + grupo, Duration.ofSeconds(60), () -> {
            try {
                return kafka.execInContainer(
                        "/opt/kafka/bin/kafka-consumer-groups.sh",
                        "--bootstrap-server", "localhost:9092",
                        "--group", grupo,
                        "--reset-offsets", "--to-earliest", "--all-topics", "--execute").getExitCode() == 0;
            } catch (Exception e) {
                return false;
            }
        });
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

    // ---- acesso ao sistema e ao banco ----

    private String criarPedido(UUID produtoId, int quantidade) throws Exception {
        String corpo = """
                {"itens":[{"produtoId":"%s","quantidade":%d,"precoUnitario":50.00}]}
                """.formatted(produtoId, quantidade);
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

    private static long contar(String sql) throws Exception {
        try (Connection conn = DriverManager.getConnection(postgresUrl, "tcc", "tcc");
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            rs.next();
            return rs.getLong(1);
        }
    }

    private static void aguardar(String descricao, Duration limite, Supplier<Boolean> condicao) throws Exception {
        long fim = System.nanoTime() + limite.toNanos();
        while (System.nanoTime() < fim) {
            if (condicao.get()) {
                return;
            }
            Thread.sleep(1000);
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
