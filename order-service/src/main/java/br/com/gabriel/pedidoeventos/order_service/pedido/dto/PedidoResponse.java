package br.com.gabriel.pedidoeventos.order_service.pedido.dto;

import br.com.gabriel.pedidoeventos.order_service.pedido.Pedido;
import br.com.gabriel.pedidoeventos.order_service.pedido.StatusPedido;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

public record PedidoResponse(
        UUID id,
        StatusPedido status,
        String motivoCancelamento,
        BigDecimal valorTotal,
        List<ItemPedidoResponse> itens
) {
    public static PedidoResponse from(Pedido pedido) {
        return new PedidoResponse(
                pedido.getId(),
                pedido.getStatus(),
                pedido.getMotivoCancelamento(),
                pedido.getValorTotal(),
                pedido.getItens().stream().map(ItemPedidoResponse::from).toList()
        );
    }
}