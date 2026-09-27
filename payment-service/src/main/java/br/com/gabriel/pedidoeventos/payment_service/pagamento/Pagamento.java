package br.com.gabriel.pedidoeventos.payment_service.pagamento;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "pagamento")
@Getter
@Setter
@NoArgsConstructor
public class Pagamento {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(nullable = false)
    private UUID pedidoId;

    @Column(nullable = false, precision = 12, scale = 2)
    private BigDecimal valor;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private StatusPagamento status;

    @Column
    private String motivoRecusa;

    @Column(nullable = false)
    private Instant dataProcessamento;

    public Pagamento(UUID pedidoId, BigDecimal valor, StatusPagamento status, String motivoRecusa, Instant dataProcessamento) {
        this.pedidoId = pedidoId;
        this.valor = valor;
        this.status = status;
        this.motivoRecusa = motivoRecusa;
        this.dataProcessamento = dataProcessamento;
    }
}
