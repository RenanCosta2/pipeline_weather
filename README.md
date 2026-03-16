# Pipeline de Dados Climáticos

Este projeto implementa um pipeline analítico para dados meteorológicos brasileiros coletados do **INMET** (Instituto Nacional de Meteorologia), estruturado em camadas Bronze, Silver e Gold na **Google Cloud Platform**.

O objetivo é coletar e processar dados climáticos históricos brutos e estruturá-los para possibilitar análises climáticas e exploração analítica posterior.

## Etapas do Projeto
- [Arquitetura](#arquitetura)

### Arquitetura

O pipeline segue uma arquitetura em camadas composta por ingestão, processamento e disponibilização de dados analíticos, estruturada no modelo **Medallion** (Bronze, Silver e Gold). 

A execução do pipeline é **orquestrada por DAGs**, responsáveis por coordenar as etapas das camadas de dados.

Os dados finais são disponibilizados para consumo em ferramentas de BI, permitindo a construção de relatórios e dashboards analíticos.

![alt text](img/arquitetura.png)