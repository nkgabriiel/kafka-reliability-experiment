package br.com.gabriel.pedidoeventos.monolito_pedidos.pagamento;

public record ResultadoPagamento(
        boolean Aprovado,
        String motivoRecusa
) {
    public static ResultadoPagamento aprovado() {
        return new ResultadoPagamento(true, null);
    }
    public static ResultadoPagamento recusado(String motivo) {
        return new ResultadoPagamento(false, motivo);
    }
}
