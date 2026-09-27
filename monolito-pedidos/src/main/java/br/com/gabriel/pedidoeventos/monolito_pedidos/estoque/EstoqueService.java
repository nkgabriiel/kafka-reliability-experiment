package br.com.gabriel.pedidoeventos.monolito_pedidos.estoque;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
@RequiredArgsConstructor
public class EstoqueService {

    private final EstoqueRepository estoqueRepository;

    @Transactional
    public void baixarEstoque(List<ItemBaixaEstoque> itens) {
        for(ItemBaixaEstoque item: itens) {
            Estoque estoque = estoqueRepository.findByProdutoId(item.produtoId())
                    .orElseThrow(() -> new EstoqueInsuficienteException(
                            "Produto %s não encontrado no estoque".formatted(item.produtoId())));

            if(estoque.getQuantidadeDisponivel() < item.quantidade()) {
                throw new EstoqueInsuficienteException(
                        "Estoque insuficiente para o produto %s (disponível: %d, solicitado: %d"
                                .formatted(item.produtoId(), estoque.getQuantidadeDisponivel(), item.quantidade()));
            }
            estoque.setQuantidadeDisponivel(estoque.getQuantidadeDisponivel() - item.quantidade());
        }
    }
}
