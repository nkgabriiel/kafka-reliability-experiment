package br.com.gabriel.pedidoeventos.payment_service.pagamento;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;

@Service
public class PagamentoMockService {

    private final BigDecimal limiteAprovacao;

    public PagamentoMockService(@Value("${pagamento.mock.limite-aprovacao}") BigDecimal limiteAprovacao ) {
        this.limiteAprovacao = limiteAprovacao;
    }

    public ResultadoPagamento processar(BigDecimal valorTotal) {
        if (valorTotal.compareTo(limiteAprovacao) > 0) {
            return ResultadoPagamento.recusado(
                    "Valor do pedido (%s) excede o limite de aprovação mockado (%s)"
                            .formatted(valorTotal, limiteAprovacao));
        }
        return ResultadoPagamento.aprovado();
    }
}
