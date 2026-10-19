package br.com.gabriel.pedidoeventos.stock_service.estoque;

import br.com.gabriel.pedidoeventos.stock_service.evento.EstoqueProcessadoEvent;

/** Resultado de processar uma reserva; {@code evento} é o evento a publicar (nulo se duplicado). */
public record ResultadoReserva(Status status, String motivo, EstoqueProcessadoEvent evento) {

    public enum Status {
        RESERVADO,
        INDISPONIVEL,
        DUPLICADO
    }

    public static ResultadoReserva reservado() {
        return new ResultadoReserva(Status.RESERVADO, null, null);
    }

    public static ResultadoReserva indisponivel(String motivo) {
        return new ResultadoReserva(Status.INDISPONIVEL, motivo, null);
    }

    public static ResultadoReserva duplicado() {
        return new ResultadoReserva(Status.DUPLICADO, null, null);
    }

    public ResultadoReserva comEvento(EstoqueProcessadoEvent evento) {
        return new ResultadoReserva(status, motivo, evento);
    }
}
