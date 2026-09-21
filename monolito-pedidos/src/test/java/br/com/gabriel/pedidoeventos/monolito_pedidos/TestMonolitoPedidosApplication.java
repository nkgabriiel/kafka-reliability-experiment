package br.com.gabriel.pedidoeventos.monolito_pedidos;

import org.springframework.boot.SpringApplication;

public class TestMonolitoPedidosApplication {

	public static void main(String[] args) {
		SpringApplication.from(MonolitoPedidosApplication::main).with(TestcontainersConfiguration.class).run(args);
	}

}
