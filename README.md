# Desafio Raízen para avaliação técnica

Neste repositório pode-se encontrar a solução do desafio proposto pela Raízen para avaliação técnica.

## Descrição do desafio

O teste consiste em desenvolver um pipeline de ETL para extrair caches de pivô
internos de relatórios consolidados disponibilizados pela agência reguladora de petróleo/combustíveis do governo brasileiro (ANP, Agência Nacional do Petróleo, Gás Natural e Biocombustíveis).

O arquivo com extensão .xls possui algumas tabelas pivô. O objetivo é desenvolver um pipeline que extrai e estrutura os dados de duas dessas tabelas:
- Vendas de combustíveis derivados de petróleo por UF e produto
- Vendas de diesel por UF e tipo
Os totais dos dados extraídos devem ser iguais aos totais das tabelas pivô.

Os dados devem ser armazenados no seguinte formato:

| Column       | Type        |
| ------------ | ----------- |
| `year_month` | `date`      |
| `uf`         | `string`    |
| `product`    | `string`    |
| `unit`       | `string`    |
| `volume`     | `double`    |
| `created_at` | `timestamp` |

Deve-se definir um squema de particionamento ou indexação.

## Tecnologias utilizadas
- Github: repositório e versionamento do projeto
- Github actions: plataforma de CI para testar o build do projeto
- Python: linguagem para processamento e manipulação de dados e arquivos
- Docker: ferramenta para criação de ambiente isolado (sistema operacional, bibliotecas, etc) chamado container para execução do pipeline
- Docker-compose: ferramenta utilizada na orquestração de vários containers
- Airflow: plataforma de gerenciamento de fluxo de trabalho

## Resolução
Na resolução do desafio, o primeiro ponto foi realizar o download do arquivo para o ambiente onde iríamos executar o ETL. Em seguida, deve-se extrair as tabelas indicadas do arquivo .xls. Nesta etapa, não foi possível realizar uma leitura direta do arquivo pois as abas desejadas estavam ocultas. Para contornar o problema, com auxílio do libreoffice, fez-se uma transformação do arquivo de .xls para .xlsx e, utilizando o pandas, foi possível realizar a leitura das abas ocultas onde estavam os dados desejados. Neste ponto, primeiro foi utilizada a biblioteca openpyxl para extrair as tabelas "DPCache_m3" e "DPCache_m3_2", porém, esse método não era performático pois consistia em ler o arquivo com todas as tabela pivô e excluir todas as abas, com exceção da aba desejada e gravar a que sobrou em um .xlsx. Esse processo demorava cerca de 4 minutos para ser realizado. Após pesquisar métodos mais performáticos, foi utilizada a função read_excel da biblioteca pandas. Com essa função é possível ler somente a tabela pivô desejada, diminuindo o tempo de execução para mais ou menos 15 segundos. Após a leitura do dado bruto com o pandas, foi realizado o ETL e, por fim, realizada a gravação do dado no formato parquet com os dados nos formatos indicados. Além disso, após a gravação, foi realizada uma etapa de disponibilização desses dados em um banco de dados, no caso, foi utilizado o SQLite, devido a simplicidade e fácil conexão com o python. Ao fim foram criadas duas tabela neste banco com os nomes "oil_derivative" e "diesel" e foi testada uma query para verificar se estava tudo correto. 

O gerenciamento do fluxo de trabalho foi realizado pela ferramenta Airflow, fazendo uso das DAGs (Directed Acyclic Graphs). Para utilizar o Airflow fez-se necessário o uso do docker-compose (arquivo docker-compose.yaml) para realizar a subida de todas as imagens e necessárias para a aplicação. A fim de configurar o ambiente, também fez-se uso do arquivo Dockerfile, para criar a imagem necessária para o pipeline executar. Com o uso das DAGs e com todas as imagens funcionando, através da porta 8080 pode-se visualizar o fluxo de trabalho bem como dar um start neste fluxo. Por fim, a fim de agregar valor ao desenvolvimento, utilizou-se o Github Actions como plataforma de CI para realizar o build do projeto.

## Pontos de melhoria
O primeiro ponto de melhoria seria na disponiblização desses dados em um banco de dados mais robusto como PostGreSQL ou Microsoft SQL Server, por exemplo. Além disso, pode-se pensar em alguma forma de backup para evitar perda de dados bem como uma melhoria no processo de gravação, para não precisar gravar todo o dado sempre que houver alguma atualização.

Um outro ponto de melhoria é com relação a qualidade dos dados. Aqui não foi realizado nenhum processo de Data Quality.

Para finalizar, no Github Actions, o build do projeto precisar ser rodado na mão. Pode-se implementar o uso de push e pull request para que esse start seja automático. Além disso, pode-se pensar em ambientes de dev, hmg e prd para realização de deploy utilizando esta ferramenta.

## Execução do projeto

1) Clonar o repositório: git clone https://github.com/FOliani/desafio_raizen.git

2) Navegar até a raíz do projeto e construi as imagens do airflow rodando o comando:
- docker-compose up airflow-init

3) Logo em seguida subir os containers do airflow rodando o comando:
- docker-compose up

3) No navegador web acessar a porta 8080 (user: airflow, password: airflow):
- localhost:8080

4) Encontrar o fluxo "ETL_Raizen" e pressionar o botão de start para iniciar o pipeline.

Para acompanhar o build pelo Github Actions, é necessário ir na aba Actions neste repositório. Em "All workflows" clicar em "CI_ETL_Raizen" e, logo após, clicar em "Run workflow".

## Conclusão
O teste foi concluído com algumas abordagens extras e alguns pontos de melhorias já mencionados. Agradeço a Raizen pela oportunidade e me coloco a disposição para quaisquer dúvidas.

### Autor
Fabio Henrique Oliani.
