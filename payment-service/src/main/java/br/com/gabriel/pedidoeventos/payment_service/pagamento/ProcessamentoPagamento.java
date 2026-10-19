package br.com.gabriel.pedidoeventos.payment_service.pagamento;

import br.com.gabriel.pedidoeventos.payment_service.evento.PagamentoProcessadoEvent;

/** Resultado de processar um pedido: o pagamento gravado e o evento a publicar. */
public record ProcessamentoPagamento(Pagamento pagamento, PagamentoProcessadoEvent evento) {
}
