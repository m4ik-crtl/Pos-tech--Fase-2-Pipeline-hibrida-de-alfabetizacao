## O que muda

<!-- Uma frase: o que esta PR entrega. -->

## Por quê

<!-- O problema que motivou a mudança. Se houver decisão arquitetural relevante,
     aponte a ADR correspondente em docs/adr/. -->

## Como validar

```bash
make testes
make batch
```

## Checklist

- [ ] Nenhum caminho absoluto introduzido (`make testes` cobre isso)
- [ ] Contratos de dados atualizados, se o schema mudou
- [ ] `docs/dicionario_dados.md` regenerado, se houve coluna nova
- [ ] Impacto em custo considerado (partição, formato, tempo de cluster)
- [ ] Nenhuma variável derivada do alvo entrou na feature store
