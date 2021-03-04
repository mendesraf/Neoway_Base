README

Implementação para o Desafio da empresa Neoway.

Descrição
Este é um pacote para manipulação de dados e persistência no Banco de Dados (BD) PostgreSQL utilizando a linguagem GOLANG e o Docker Compose.
Após a persistência, pode ser realizada a higienização dos dados, além da validação dos CPFs e CNPJs.

Pré-requisitos:
- Ter o Docker instalado;
- Ter o GO instalado.

Funcionalidades disponíveis:
- Tela inicial: http://localhost:8000/
- Ler o arquivo e persistir no BD: http://localhost:8000/processar
- Realizar a higienização dos dados: http://localhost:8000/higienizar

Instalação (Windows) e Orientações de Uso:

1. Baixar o projeto na máquina local;

2. Acessar o diretório do projeto pelo Prompt do Windows e executar o comando abaixo para realizar o build do projeto no Docker Compose:
    docker-compose build

3. Executar o comando abaixo para rodar o projeto: 
    docker-compose up

4. Para acessar o servidor do BD, basta executar o comando abaixo no Prompt do Windows:
    docker -it dockerdev-bd bash
4.1 Executar o comando abaixo para conectar no BD e poder realizar as consultas:
    psql -U goland -d goland
4.2 Realizar a criação da tabela e dos índices.

5. No navegador, utilizar a url abaixo para ir à tela inicial: 
    http://localhost:8000/

6. Se não houver registros na tabela, deve ser realizado o processamento do arquivo através da url abaixo:
    http://localhost:8000/processar
    OBS: o arquivo possui cerca de 50.000 linhas e esse processo pode ser demorado
6.1 Caso queira carregar novamente o arquivo, é só acessar a url novamente, pois o processamento consiste em truncar a tabela (deletar todos os registros) e realizar os inserts novamente.

7. Para encerrar a execução do projeto, basta voltar ao Prompt do Windows do item 3 e usar as teclas Crtl+C.


Attachment I

Part of this repository contains samples for how to work with Go and Docker from GoLand IDE.

### License
Copyright 2019 Florin Pățan

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.