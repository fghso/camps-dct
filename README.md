camps-crawler
=====

Os arquivos python no diretório raiz deste projeto definem o coletor genérico, denominado *camps-crawler*. Os demais diretórios contêm arquivos referentes à coleta de dados específica do Instagram, cujo projeto foi denominado *instagram-crawler*.

Para utilizar o coletor genérico, basta executar o servidor (`server.py`) e o cliente (`client.py`) em qualquer máquina. O servidor espera receber como entrada o caminho para um arquivo de configuração XML, que deve estar de acordo com o formato abaixo:

```xml
<?xml version="1.0" encoding="ISO8859-1" ?>
<config>
    <connection>
        <address>server_address</address>
        <port>port_number</port>
        <bufsize>buffer_size</bufsize>
    </connection>
    <database>
        <user>db_user</user>
        <password>db_password</password>
        <host>db_host</host>
        <name>db_name</name>
        <table>db_table_name</table>
    </database>
</config>
```

Além do endereço e porta de conexão, o arquivo de configuração especifica o banco de dados MySQL que será usado para obter as informações dos recursos a serem coletados. É necessário designar a tabela onde essas informações estão armazenadas. A tabela deve possuir os seguintes campos:

| resource_id  | status | amount | crawler | updated_at |
| ------------ | ------ | ------ | ------- | ---------- |

Abaixo está um exemplo de como essa tabela pode ser criada:

```sql
CREATE TABLE `resources` (
    `resources_pk` int(10) unsigned NOT NULL AUTO_INCREMENT,
    `resource_id` int(10) unsigned NOT NULL,
    `status` tinyint(5) NOT NULL DEFAULT '0',
    `response_code` tinyint(5) DEFAULT NULL,
    `annotation` varchar(200) DEFAULT NULL,
    `crawler` varchar(45) DEFAULT NULL,
    `updated_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`resources_pk`),
    UNIQUE KEY `resource_id_UNIQUE` (`resource_id`)
);
```

O servidor espera que a tabela já esteja populada com os IDs dos recursos quando o programa for iniciado, isto é, a tabela deve ser populada antes de iniciar a coleta. Os IDs podem ser qualquer informação que faça sentido para o código que efetivamente fará a coleta (podem ser, por exemplo, identificadores de usuários, hashtags, URLs, etc.). O servidor apenas gerencia qual ID está designado para cada cliente.

Assim como o servidor, o cliente espera receber como entrada o caminho para um arquivo de configuração XML, também de acordo com o formato descrito acima. No caso do cliente, porém, apenas as informações de conexão são necessárias. O endereço e porta informados devem ser os do servidor.

No mesmo diretório de execução do cliente deve existir um arquivo com o nome `crawler.py`. Esse é o arquivo que conterá o código de coleta. O arquivo deve seguir o template contido no `crawler.py` que está no repositório. 


Gerenciamento
-----

Para saber a situação atual da coleta, é possível utilizar o programa `manager.py`. Ele deve ser chamado de maneira similar ao cliente, designando-se o caminho para o arquivo de configuração XML que contém as informações de conexão com o servidor.

Os seguintes argumentos opcionais estão disponíveis para esse programa:

```
-h, --help                      Mostra a mensagem de ajuda e sai
-s, --status                    Obtém o status atual de todos os clientes conectados ao servidor
-r clientID, --remove clientID  Remove da lista do servidor o cliente com o ID especificado
--shutdown                      Remove todos os clientes da lista do servidor e o desliga
```

A penúltima opção (`-r clientID, --remove clientID`) permite excluir um cliente da lista do servidor informando seu ID. O cliente pode estar ativo ou inativo quando for excluído (se estiver ativo, assim que fizer um novo contato com o servidor irá receber deste uma mensagem para que termine sua execução). De maneira semelhante, no caso da última opção (`--shutdown`) o servidor notifica todos os clientes ativos para que terminem sua execução antes que o próprio servidor seja desligado.

Se nenhum argumento opcional for informado, o programa, por padrão, exibe o status.


Fluxo básico
-----

De maneira suscinta, o fluxo de funcionamento do coletor é o seguinte: 

1. Inicia-se o servidor
2. O servidor aguarda novas conexões de clientes
3. Inicia-se o cliente
4. O cliente conversa com o servidor, identifica-se e, em seguida, pede um novo ID para coletar
5. O servidor procura um ID ainda não coletado no banco de dados e o repassa ao cliente, marcando esse ID com o status *sendo coletado*
6. O cliente recebe o ID e chama a função `crawl(resourceID)`, da classe `Crawler`, que deve estar contida no arquivo `crawler.py`
7. A função `crawl(resourceID)` faz a coleta do recurso e retorna o status e a quantidade para o cliente
8. O cliente repassa esses valores para o servidor
9. O servidor marca o recurso com o valor de status recebido do cliente e armazena também o valor de quantidade informado
10. O cliente pede um novo ID ao servidor para coletar e o fluxo retorna ao passo 5

Como a ideia do coletor é proporcionar uma coleta distribuída, pode-se iniciar vários clientes simultaneamente, a partir de qualquer máquina na mesma rede do servidor.
    