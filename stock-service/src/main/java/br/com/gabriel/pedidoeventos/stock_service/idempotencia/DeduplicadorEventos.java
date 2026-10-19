package br.com.gabriel.pedidoeventos.stock_service.idempotencia;

import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Component
@RequiredArgsConstructor
public class DeduplicadorEventos {

    private final EventoProcessadoRepository repository;

    @Value("${app.protecao.dedupe:false}")
    private boolean ativo;

    /**
     * Registra o evento como processado. Devolve true se ele é novo e false se já tinha sido
     * processado. Precisa rodar na mesma transação que aplica o efeito de negócio, por isso
     * exige uma transação já aberta.
     * Com a proteção desligada (versões v1 e v2), todo evento é tratado como novo e nada é gravado.
     */
    @Transactional(propagation = Propagation.MANDATORY)
    public boolean registrarSeNovo(UUID eventId) {
        if (!ativo) {
            return true;
        }
        return repository.inserirSeAusente(eventId) == 1;
    }
}
