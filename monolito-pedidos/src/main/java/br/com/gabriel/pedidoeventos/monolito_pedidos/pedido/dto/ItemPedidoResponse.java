package br.com.gabriel.pedidoeventos.monolito_pedidos.pedido.dto;

import br.com.gabriel.pedidoeventos.monolito_pedidos.pedido.ItemPedido;

import java.math.BigDecimal;
import java.util.UUID;

public record ItemPedidoResponse(
        UUID produtoId,
        Integer quantidade,
        BigDecimal precoUnitario
) {
    public static ItemPedidoResponse from(ItemPedido item) {
        return new ItemPedidoResponse(item.getProdutoId(), item.getQuantidade(), item.getPrecoUnitario());
    }
}
