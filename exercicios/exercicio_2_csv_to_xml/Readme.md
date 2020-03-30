## Passo a passo para resolver o exercicio 2 

Nesta pasta abrir um terminal linux e executar
```
pyspark --packages com.databricks:spark-xml_2.12:0.6.0
```

![](imagens/foto1.png)

```
import pyspark
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from os.path import  exists, isdir
from os import makedirs, mkdir

spark=SparkSession.builder.appName("experian").getOrCreate()
```
Carregando um arquivo .xml para teste
```
fx_1 = spark.read.format('com.databricks.spark.xml').options(rowTag='book').load("books.xml")

fx_1.show()
```

![](imagens/foto2.png)

```
df = spark.read.format('csv').options(header='true', inferSchema='true', delimiter = ";").load('../../viagens_csv/2020_Viagem.csv')

for col in df.columns:
   df = df.withColumnRenamed(col,col.replace(" ", "_"))

df = df.withColumnRenamed('Situa��o','Situacao')\
        .withColumnRenamed('C�digo_do_�rg�o_superior','Codigo_do_orgao_superior')\
        .withColumnRenamed('Nome_do_�rg�o_superior','Nome_do_orgao_superior')\
        .withColumnRenamed('C�digo_�rg�o_solicitante','Codigo_orgao_solicitante')\
        .withColumnRenamed('Nome_�rg�o_solicitante','Nome_orgao_solicitante')\
        .withColumnRenamed('Per�odo_-_Data_de_in�cio','Periodo_-_Data_de_inicio')\
        .withColumnRenamed('Per�odo_-_Data_de_fim','Periodo_-_Data_de_fim')\
        .withColumnRenamed('Valor_di�rias','Valor_diarias') 

df.printSchema()
```
![](imagens/foto3.png)

Escrevendo o resultado no formato .xml

```
if not exists('../resultados/res_2/'):
    makedirs('../resultados/res_2/')

df.write.format("com.databricks.spark.xml").option("rootTag", "Serasa").option("rowTag", "passagem").save("../resultados/res_2/passagem.xml")
```
