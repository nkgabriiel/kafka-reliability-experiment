package br.com.gabriel.pedidoeventos.monolito_pedidos.estoque;

public class EstoqueInsuficienteException extends RuntimeException {
    public EstoqueInsuficienteException(String message) {
        super(message);
    }
}
