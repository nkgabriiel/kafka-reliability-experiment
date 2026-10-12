package br.com.gabriel.pedidoeventos.stock_service.idempotencia;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.UUID;

public interface EventoProcessadoRepository extends JpaRepository<EventoProcessado, UUID> {

    @Modifying
    @Query(value = "INSERT INTO eventos_processados (event_id, processado_em) VALUES (:eventId, now()) "
            + "ON CONFLICT (event_id) DO NOTHING", nativeQuery = true)
    int inserirSeAusente(@Param("eventId") UUID eventId);
}
