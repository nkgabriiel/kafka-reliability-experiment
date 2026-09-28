package br.com.gabriel.pedidoeventos.stock_service.evento;

import java.util.UUID;

public record ItemReservaEvento(
        UUID produtoId,
        Integer quantidade
) {
}
