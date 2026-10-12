package br.com.gabriel.pedidoeventos.stock_service.outbox;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface OutboxEventRepository extends JpaRepository<OutboxEvent, UUID> {

    /** Pendentes mais antigos primeiro, para preservar a ordem de criação dentro do lote. */
    List<OutboxEvent> findTop50ByEnviadoEmIsNullOrderByCriadoEmAsc();
}
