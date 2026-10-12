package br.com.gabriel.pedidoeventos.stock_service.idempotencia;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "eventos_processados")
@Getter
@Setter
@NoArgsConstructor
public class EventoProcessado {

    @Id
    private UUID eventId;

    @Column(nullable = false)
    private Instant processadoEm;
}
