# Como contribuir

Projeto acadêmico (Pós Tech FIAP), mas mantido com as mesmas práticas de um
repositório real. Antes de abrir um Pull Request:

## Checklist

- [ ] `pytest -q` passa localmente (26 testes) — inclui
      `test_nenhum_caminho_absoluto_no_codigo`, que varre `src/` e `config/`
      atrás de caminhos absolutos (`C:\Users\...`, `/home/...` etc.).
- [ ] Nenhum arquivo novo tem caminho absoluto pessoal, e-mail ou nome de
      máquina do seu computador — nem em código, nem em documentação
      (`docs/`, `README.md`). Prefira instruções relativas
      (`git clone ... && cd Fiap-tech-2`) a um `cd` fixo.
- [ ] Nenhuma credencial, token ou segredo em texto — use `.env` (fora do
      controle de versão; veja `.env.example`).
- [ ] Documentação que só ajuda **você** a preparar uma entrega (roteiro de
      vídeo, anotações pessoais) fica fora do repositório, não em `docs/`.
- [ ] `ruff check .` sem erros.

## Por que isso está aqui

Uma versão anterior deste repositório publicou, sem querer, o caminho local
do Windows de quem estava rodando o projeto dentro de `docs/guia_execucao.md`
— commitado em vários commits do histórico. O reparo exigiu reescrever o
histórico do zero. Este checklist existe para que não aconteça de novo.
