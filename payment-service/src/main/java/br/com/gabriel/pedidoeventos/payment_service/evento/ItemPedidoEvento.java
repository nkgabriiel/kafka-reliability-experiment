package br.com.gabriel.pedidoeventos.payment_service.evento;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

public record ItemPedidoEvento(
        UUID produtoId,
        Integer quantidade,
        BigDecimal precoUnitario
) {

}
