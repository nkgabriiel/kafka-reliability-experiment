package br.com.gabriel.pedidoeventos.stock_service.event;

import java.time.Instant;
import java.util.UUID;

public record EstoqueProcessadoEvent(
        UUID eventId,
        UUID pedidoId,
        StatusProcessamentoFinal status,
        String motivo,
        Instant dataProcessamento
) {
}