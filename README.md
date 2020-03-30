# pyspark_exemplos
Projeto com exemplos de problemas de análise e manipulação de dados com python e pyspark

## Instalação passa-a-passo do Pyspark no linux LTS 16.04

### 1. Instalando python3.6
Uma boa prática de programação é criar um ambiente isolado de desenvolvimento para o Python3 e suas dependências (Virtual Environments - env). Abaixo alguns tutoriais de como criar um ambiente virtual.

https://virtualenvwrapper.readthedocs.io/en/latest/
https://pythonacademy.com.br/blog/python-e-virtualenv-como-programar-em-ambientes-virtuais

### 2. Download Spark

O spark é um projeto Open Source disponibilizado pela Apache Software Foundation. Nesse estudo foi usado o pacote 'spark-3.0.0-preview2-bin-hadoop2.7'. Mova para a pasta ~/home/spark o conteudo do donwload. Segue o link para download.

http://spark.apache.org/downloads.html

### 3. Instalando o pyspark

Entre na sua env criada no passo 1. Execute o comando com o pip
$ pip install pyspark
Após terminar a instalação acima, abrir no seu editor de texto o  ~/.bashrc e copiar os códigos abaixo
Encontre a versão do py4j na pasta spark/python/lib, para colocar no codigo abaixo ex: py4j-0.10.8.1-src.zip 
```
export SPARK_HOME="/home/spark/"
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-<version>-src.zip:$PYTHONPATH
export PATH="$SPARK_HOME/bin:$PATH" 

function snotebook () 
{
#Spark path (based on your computer)
SPARK_PATH=/home/spark/

export PYSPARK_DRIVER_PYTHON="jupyter"
export PYSPARK_DRIVER_PYTHON_OPTS="notebook"

# For python 3 users, you have to add the line below or you will get an error 
export PYSPARK_PYTHON=python3

$SPARK_PATH/bin/pyspark --master local[2]
} 
```
Execute o comando
```
source ~/.bashrc
```

Configurar o spark para rodar no python3. Encontre o arquivo /home/spark/conf/spark-env.sh e adicione as linhas abaixo.
```
export PYSPARK_PYTHON=/usr/local/bin/python3
export PYSPARK_DRIVER_PYTHON=python3 
```
### 4. Executando o Pyspark

Execute no terminal o comando 
```
pyspark
```
Teste o Pyspark
```
import pyspark

spark = pyspark.sql.SparkSession.builder.appName('test').getOrCreate()
spark.range(10).collect() 

```
### Inicialize o jupyter-notebook e teste o pyspark como acima!

### Parabens!! O ambiente esta pronto para o estudo!! \0/


## Referencias:

http://spark.apache.org/downloads.html

https://pypi.org/project/pyspark/

https://medium.com/tinghaochen/how-to-install-pyspark-locally-94501eefe421

https://medium.com/@brajendragouda/installing-apache-spark-on-ubuntu-pyspark-on-juputer-ca8e40e8e655

https://medium.com/@GalarnykMichael/install-spark-on-ubuntu-pyspark-231c45677de0

https://stackoverflow.com/questions/46286436/running-pyspark-after-pip-install-pyspark/49587560

