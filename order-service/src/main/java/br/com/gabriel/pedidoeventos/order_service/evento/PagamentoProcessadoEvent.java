package br.com.gabriel.pedidoeventos.order_service.evento;

import java.time.Instant;
import java.util.UUID;

public record PagamentoProcessadoEvent(
        UUID eventId,
        UUID pedidoId,
        StatusPagamento status,
        String motivoRecusa,
        Instant dataProcessamento
) {
}
