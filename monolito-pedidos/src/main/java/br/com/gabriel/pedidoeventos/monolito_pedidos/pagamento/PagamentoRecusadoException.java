package br.com.gabriel.pedidoeventos.monolito_pedidos.pagamento;

public class PagamentoRecusadoException extends RuntimeException {
    public PagamentoRecusadoException(String message) {
        super(message);
    }
}
