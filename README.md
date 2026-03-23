# Pipeline de Dados Climáticos

Este projeto implementa um pipeline analítico para dados meteorológicos brasileiros coletados do **INMET** (Instituto Nacional de Meteorologia), estruturado em camadas Bronze, Silver e Gold na **Google Cloud Platform**.

O objetivo é coletar e processar dados climáticos históricos brutos e estruturá-los para possibilitar análises climáticas e exploração analítica posterior.

## Etapas do Projeto
- [Arquitetura](#arquitetura)
- [Coleta](#coleta)

### Arquitetura

O pipeline segue uma arquitetura em camadas composta por ingestão, processamento e disponibilização de dados analíticos, estruturada no modelo **Medallion** (Bronze, Silver e Gold). 

A execução do pipeline é **orquestrada por DAGs**, responsáveis por coordenar as etapas das camadas de dados.

Os dados finais são disponibilizados para consumo em ferramentas de BI, permitindo a construção de relatórios e dashboards analíticos.

![alt text](img/arquitetura.png)

### Coleta de Dados


A fonte de dados deste projeto é o [Banco de Dados Meteorológicos do INMET](https://bdmep.inmet.gov.br), disponível publicamente por meio do portal oficial. 

Os dados contemplam [séries históricas anuais](https://portal.inmet.gov.br/uploads/dadoshistoricos/) de estações meteorológicas distribuídas em todo o território brasileiro, abrangendo o período de 2000 até os dias atuais. Entre as principais variáveis disponíveis estão:

- Pressão atmosférica
- Radiação Global
- Temperatura
- Umidade relativa do ar
- Velocidade e direção do vento

A coleta dos dados é realizada por meio de requisições HTTP para endpoints anuais disponibilizados pelo INMET. Cada requisição retorna um arquivo compactado (`.zip`) contendo os dados de todas as estações meteorológicas para o respectivo ano.

Exemplo de endpoint:

```
https://portal.inmet.gov.br/uploads/dadoshistoricos/{ano}.zip
```

Esse arquivo compactado corresponde aos dados da camada `raw`, ou camada de dados brutos.