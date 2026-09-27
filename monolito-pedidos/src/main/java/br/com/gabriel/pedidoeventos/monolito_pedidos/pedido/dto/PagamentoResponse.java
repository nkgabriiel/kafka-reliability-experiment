package br.com.gabriel.pedidoeventos.monolito_pedidos.pedido.dto;

import br.com.gabriel.pedidoeventos.monolito_pedidos.pagamento.Pagamento;
import br.com.gabriel.pedidoeventos.monolito_pedidos.pagamento.StatusPagamento;

import java.math.BigDecimal;
import java.util.UUID;

public record PagamentoResponse (
        UUID id,
        StatusPagamento status,
        BigDecimal valor
) {

    public static PagamentoResponse from(Pagamento pagamento) {
        return new PagamentoResponse(pagamento.getId(), pagamento.getStatus(), pagamento.getValor());
    }
}
