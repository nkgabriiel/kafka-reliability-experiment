package br.com.gabriel.pedidoeventos.stock_service.estoque;

public record ResultadoReserva(Status status, String motivo) {

    public enum Status {
        RESERVADO,
        INDISPONIVEL,
        DUPLICADO
    }

    public static ResultadoReserva reservado() {
        return new ResultadoReserva(Status.RESERVADO, null);
    }

    public static ResultadoReserva indisponivel(String motivo) {
        return new ResultadoReserva(Status.INDISPONIVEL, motivo);
    }

    public static ResultadoReserva duplicado() {
        return new ResultadoReserva(Status.DUPLICADO, null);
    }
}
