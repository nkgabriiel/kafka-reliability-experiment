package br.com.gabriel.pedidoeventos.order_service.idempotencia;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Component
@RequiredArgsConstructor
public class DeduplicadorEventos {

    private final EventoProcessadoRepository repository;

    /**
     * Registra o evento como processado. Devolve true se ele é novo e false se já tinha sido
     * processado. Precisa rodar na mesma transação que aplica o efeito de negócio, por isso
     * exige uma transação já aberta.
     */
    @Transactional(propagation = Propagation.MANDATORY)
    public boolean registrarSeNovo(UUID eventId) {
        return repository.inserirSeAusente(eventId) == 1;
    }
}
