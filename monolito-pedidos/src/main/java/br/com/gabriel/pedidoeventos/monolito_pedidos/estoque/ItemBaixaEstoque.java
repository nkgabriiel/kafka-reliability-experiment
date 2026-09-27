package br.com.gabriel.pedidoeventos.monolito_pedidos.estoque;

import java.util.UUID;

public record ItemBaixaEstoque(
        UUID produtoId,
        Integer quantidade
) {
}
