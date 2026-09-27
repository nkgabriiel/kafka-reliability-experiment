package br.com.gabriel.pedidoeventos.order_service.evento;

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
