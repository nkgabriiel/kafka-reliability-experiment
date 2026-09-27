package br.com.gabriel.pedidoeventos.payment_service.evento;

import br.com.gabriel.pedidoeventos.payment_service.pagamento.StatusPagamento;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public record PagamentoProcessadoEvent(
        UUID eventId,
        UUID pedidoId,
        StatusPagamento status,
        String motivoRecusa,
        List<ItemReservaEvento> itens,
        Instant dataProcessamento
) {
}