package br.com.gabriel.pedidoeventos.stock_service.estoque;

public class EstoqueInsuficienteException extends RuntimeException {
    public EstoqueInsuficienteException(String message) {
        super(message);
    }
}
