package br.com.gabriel.pedidoeventos.order_service.pedido.dto;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;

import java.util.List;

public record PedidoRequest(

        @NotEmpty(message = "o pedido deve ter ao menos um item")
        @Valid
        List<ItemPedidoRequest> itens
) {
}