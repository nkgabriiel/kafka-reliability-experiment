package br.com.gabriel.pedidoeventos.monolito_pedidos.pagamento;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

public interface PagamentoRepository extends JpaRepository<Pagamento, UUID> {
}
