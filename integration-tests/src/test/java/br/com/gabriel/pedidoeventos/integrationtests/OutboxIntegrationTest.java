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
 * Fase 6 (Versão 4): transactional outbox.
 *
 * Reproduz a janela de perda da V3: o payment-service grava o pagamento e cai depois do commit e
 * antes de publicar o evento seguinte. O poller da outbox do payment-service é segurado por 45 s
 * depois de cada subida, e o ack da mensagem de entrada é atrasado em 30 s, de modo que o kill cai
 * depois do commit, antes do ack e antes da publicação. Ao reiniciar, a mensagem é reentregue e
 * descartada pelo dedupe (V3), e o evento que ficou na outbox tem de ser publicado (V4).
 */
class OutboxIntegrationTest {

    static ComposeContainer ambiente = new ComposeContainer(new File("../docker-compose.yml"))
            .withBuild(true)
            .withEnv("ATRASO_ACK_MS", "30000")
            .withEnv("SESSION_TIMEOUT_MS", "6000")
            .withEnv("HEARTBEAT_MS", "1000")
            .withEnv("PAYMENT_OUTBOX_ATRASO_INICIAL_MS", "45000")
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
    void killEntreCommitEPublicacao_eventoNaoSePerdeEPublicaAoReiniciar() throws Exception {
        aguardar("consumers prontos (partições atribuídas)", Duration.ofSeconds(120), () ->
                ocorrencias("payment-service", "partitions assigned") >= 1
                        && ocorrencias("stock-service", "partitions assigned") >= 1
                        && ocorrencias("order-service", "partitions assigned") >= 2);

        // reinicia o payment-service para a contagem dos 45 s do poller começar agora, com folga
        // para criar o pedido e matar o serviço antes de a primeira varredura rodar
        parar("payment-service");
        iniciar("payment-service");
        aguardar("payment-service pronto de novo", Duration.ofSeconds(120),
                () -> ocorrencias("payment-service", "partitions assigned") >= 2);

        UUID produtoId = UUID.randomUUID();
        executar("INSERT INTO estoque.estoque (id, produto_id, nome_produto, quantidade_disponivel) "
                + "VALUES (gen_random_uuid(), '" + produtoId + "', 'Produto Outbox', 10)");

        String pedidoId = criarPedido(produtoId, 2);

        // o commit aconteceu quando a linha da outbox aparece; o poller ainda está segurado
        String evento = "SELECT COUNT(*) FROM pagamento.outbox_events WHERE payload LIKE '%" + pedidoId + "%'";
        aguardar("pagamento commitado com o evento na outbox", Duration.ofSeconds(30),
                () -> contar(evento) == 1);
        matar("payment-service");

        // estado deixado pela queda: pagamento gravado, evento ainda não publicado
        assertEquals(1, contar("SELECT COUNT(*) FROM pagamento.pagamento WHERE pedido_id = '" + pedidoId + "'"));
        assertEquals(1, contar(evento + " AND enviado_em IS NULL"),
                "o evento deveria estar na outbox, ainda não publicado");
        assertEquals("PENDENTE", statusDoPedido(pedidoId));

        iniciar("payment-service");

        // a mensagem pedidos.criados foi reentregue (o ack não tinha sido dado) e descartada pelo
        // dedupe; o evento que sobrou na outbox é publicado pelo poller depois dos 45 s
        aguardar("pedido concluído depois do restart", Duration.ofSeconds(180),
                () -> "CONCLUIDO".equals(statusDoPedido(pedidoId)));

        assertTrue(ocorrencias("payment-service", "descartando sem reprocessar") >= 1,
                "a reentrega da mensagem de entrada deveria ter sido descartada pelo dedupe");
        assertEquals(1, contar("SELECT COUNT(*) FROM pagamento.pagamento WHERE pedido_id = '" + pedidoId + "'"),
                "o restart não pode gerar um segundo pagamento");
        assertEquals(8, contar("SELECT quantidade_disponivel FROM estoque.estoque WHERE produto_id = '" + produtoId + "'"),
                "o estoque deve ser reservado exatamente uma vez");
        assertEquals(1, contar(evento + " AND enviado_em IS NOT NULL"),
                "o evento da outbox deve ter sido marcado como enviado");
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

    private static void matar(String servico) {
        ContainerState c = container(servico);
        c.getDockerClient().killContainerCmd(c.getContainerId()).exec();
    }

    private static void iniciar(String servico) {
        ContainerState c = container(servico);
        c.getDockerClient().startContainerCmd(c.getContainerId()).exec();
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
