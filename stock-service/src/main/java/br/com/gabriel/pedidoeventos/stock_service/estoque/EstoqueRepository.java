package br.com.gabriel.pedidoeventos.stock_service.estoque;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;
import java.util.UUID;

public interface EstoqueRepository extends JpaRepository<Estoque, UUID> {
    Optional<Estoque> findByProdutoId(UUID produtoId);
}
