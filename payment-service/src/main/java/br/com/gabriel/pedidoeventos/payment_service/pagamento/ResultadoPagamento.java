package br.com.gabriel.pedidoeventos.payment_service.pagamento;

public record ResultadoPagamento(
        boolean sucesso,
        String motivoRecusa)
{
    public static ResultadoPagamento aprovado() {
        return new ResultadoPagamento(true, null);
    }
    public static ResultadoPagamento recusado(String motivo) {
        return new ResultadoPagamento(false, motivo);
    }
}

