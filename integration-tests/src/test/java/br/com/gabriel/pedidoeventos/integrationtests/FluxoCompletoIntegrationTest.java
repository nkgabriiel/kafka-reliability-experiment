package br.com.gabriel.pedidoeventos.integrationtests;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.ComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.File;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Duration;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FluxoCompletoIntegrationTest {

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
    void deveCriarPedidoEConcluirComSucesso() throws Exception {
        UUID produtoId = UUID.randomUUID();
        try (Connection conn = DriverManager.getConnection(postgresUrl + "?currentSchema=estoque", "tcc", "tcc");
             Statement stmt = conn.createStatement()) {
            stmt.execute("INSERT INTO estoque.estoque (id, produto_id, nome_produto, quantidade_disponivel) VALUES (gen_random_uuid(), '"
                    + produtoId + "', 'Produto Integration Test', 10)");
        }

        HttpClient client = HttpClient.newHttpClient();
        String corpo = """
                {"itens":[{"produtoId":"%s","quantidade":2,"precoUnitario":50.00}]}
                """.formatted(produtoId);

        HttpResponse<String> criarResponse = client.send(
                HttpRequest.newBuilder(URI.create(orderServiceUrl + "/pedidos"))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(corpo))
                        .build(),
                HttpResponse.BodyHandlers.ofString());

        assertEquals(202, criarResponse.statusCode());
        String pedidoId = criarResponse.body().split("\"id\":\"")[1].split("\"")[0];

        String status = "PENDENTE";
        for (int tentativa = 0; tentativa < 20 && "PENDENTE".equals(status); tentativa++) {
            Thread.sleep(500);
            HttpResponse<String> statusResponse = client.send(
                    HttpRequest.newBuilder(URI.create(orderServiceUrl + "/pedidos/" + pedidoId)).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            status = statusResponse.body().split("\"status\":\"")[1].split("\"")[0];
        }

        assertEquals("CONCLUIDO", status);
    }
}