package br.com.gabriel.pedidoeventos.monolito_pedidos.pagamento;

import br.com.gabriel.pedidoeventos.monolito_pedidos.pedido.Pedido;
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

    @OneToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "pedido_id", nullable = false, unique = true)
    private Pedido pedido;

    @Column(nullable = false, precision = 12, scale = 2)
    private BigDecimal valor;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private StatusPagamento status;

    @Column(nullable = false)
    private Instant dataProcessamento;

    public Pagamento(BigDecimal valor, StatusPagamento status, Instant dataProcessamento) {
        this.valor = valor;
        this.status = status;
        this.dataProcessamento = dataProcessamento;
    }
}
