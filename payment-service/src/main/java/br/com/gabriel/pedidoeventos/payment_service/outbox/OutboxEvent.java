package br.com.gabriel.pedidoeventos.payment_service.outbox;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.Instant;
import java.util.UUID;

/** Evento a publicar no Kafka, gravado na mesma transação que a mudança de estado de negócio. */
@Entity
@Table(name = "outbox_events")
@Getter
@Setter
@NoArgsConstructor
public class OutboxEvent {

    /** O mesmo eventId do evento, para que o dedupe dos consumers continue valendo. */
    @Id
    private UUID id;

    @Column(nullable = false)
    private String topico;

    @Column(nullable = false)
    private String chave;

    /** Nome completo da classe do evento, usado para reconstruí-lo na hora de publicar. */
    @Column(nullable = false)
    private String tipo;

    @Column(nullable = false, columnDefinition = "text")
    private String payload;

    @Column(nullable = false)
    private Instant criadoEm;

    /** Nulo enquanto o evento não foi publicado com confirmação do broker. */
    private Instant enviadoEm;
}
