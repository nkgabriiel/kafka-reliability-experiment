package br.com.gabriel.pedidoeventos.order_service.evento;

import java.math.BigDecimal;
import java.util.UUID;

public record ItemPedidoEvento (
        UUID produtoId,
        Integer quantidade,
        BigDecimal precoUnitario
) {

}
