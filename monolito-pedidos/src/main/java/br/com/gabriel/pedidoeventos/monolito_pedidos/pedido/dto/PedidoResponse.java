package br.com.gabriel.pedidoeventos.monolito_pedidos.pedido.dto;

import br.com.gabriel.pedidoeventos.monolito_pedidos.pedido.Pedido;
import br.com.gabriel.pedidoeventos.monolito_pedidos.pedido.StatusPedido;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

public record PedidoResponse(
        UUID id,
        StatusPedido status,
        BigDecimal valorTotal,
        List<ItemPedidoResponse> itens,
        PagamentoResponse pagamento
) {
    public static PedidoResponse from(Pedido pedido) {
        return new PedidoResponse(
                pedido.getId(),
                pedido.getStatus(),
                pedido.getValorTotal(),
                pedido.getItens().stream().map(ItemPedidoResponse::from).toList(),
                pedido.getPagamento() != null ? PagamentoResponse.from(pedido.getPagamento()) : null
        );
    }
}
