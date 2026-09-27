package br.com.gabriel.pedidoeventos.monolito_pedidos.pedido.dto;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

import java.math.BigDecimal;
import java.util.UUID;

public record ItemPedidoRequest(
        @NotNull(message = "produtoId é obrigatório")
        UUID produtoId,

        @NotNull(message = "quantidade é obrigatória")
        @Min(value = 1, message = "quantidade deve ser maior que zero")
        Integer quantidade,

        @NotNull(message = "precoUnitario é obrigatório")
        @DecimalMin(value = "0.01", message = "precoUnitario deve ser maior que zero")
        BigDecimal precoUnitario
) {
}
