package br.com.gabriel.pedidoeventos.monolito_pedidos;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

@Import(TestcontainersConfiguration.class)
@SpringBootTest
class MonolitoPedidosApplicationTests {

	@Test
	void contextLoads() {
	}

}
