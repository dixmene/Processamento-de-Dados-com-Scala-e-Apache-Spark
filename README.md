
## **Processamento de Dados com Scala e Apache Spark**

### **1. Introdução**

- **Objetivo**: Utilizar Apache Spark com Scala para processar e analisar grandes volumes de dados, realizando transformações e agregações para gerar insights significativos.
- **Ferramentas**: Apache Spark, Scala.

---

### **2. Configuração do Ambiente**

- **Passo 1**: Configuração do Spark
  ```scala
  import org.apache.spark.sql.SparkSession
  
  // Configuração do ambiente Spark
  val spark = SparkSession.builder()
    .appName("DataProcessingWithSpark")
    .config("spark.master", "local")
    .getOrCreate()
  
  println(s"Spark Version: ${spark.version}")
  ```
  - **Descrição**: Inicializamos o ambiente Spark e verificamos a versão para garantir que tudo está configurado corretamente.

---

### **3. Carregamento dos Datasets**

- **Passo 2**: Carregamento e Verificação dos Dados
  ```scala
  val aisles = spark.read.option("header", "true").csv("/path/to/aisles.csv")
  val departments = spark.read.option("header", "true").csv("/path/to/departments.csv")
  val orderProductsPrior = spark.read.option("header", "true").csv("/path/to/order_products_prior.csv")
  val orderProductsTrain = spark.read.option("header", "true").csv("/path/to/order_products_train.csv")
  val orders = spark.read.option("header", "true").csv("/path/to/orders.csv")
  val products = spark.read.option("header", "true").csv("/path/to/products.csv")
  
  // Exibir o esquema dos datasets
  aisles.printSchema()
  departments.printSchema()
  orderProductsPrior.printSchema()
  orderProductsTrain.printSchema()
  orders.printSchema()
  products.printSchema()
  ```
  - **Descrição**: Carregamos diversos datasets e verificamos seus esquemas para assegurar que a estrutura dos dados está correta.

---

### **4. Operações de Transformação**

- **Passo 3**: Transformações dos Dados
  ```scala
  import org.apache.spark.sql.functions._
  
  // Unir datasets de produtos e realizar transformação
  val orderProductsCombined = orderProductsPrior.union(orderProductsTrain)
  val orderCounts = orderProductsCombined.groupBy("order_id").agg(count("product_id").alias("product_count"))
  
  // Exibir dados transformados
  orderCounts.show(5)
  ```
  - **Descrição**: Unimos os dados dos produtos e agregamos o número de produtos por pedido para análise posterior.

---

### **5. Operações de Agregação**

- **Passo 4**: Agregações Usando RDD
  ```scala
  // Converter DataFrame para RDD e realizar agregação
  val ordersRDD = orderCounts.rdd.map(row => (row.getAs[String]("order_id"), row.getAs[Long]("product_count")))
  
  // Contar o número de pedidos para cada número de produtos
  val countByProductCount = ordersRDD.map { case (_, count) => (count, 1) }.reduceByKey(_ + _)
  
  // Exibir resultados
  countByProductCount.collect().foreach(println)
  ```
  - **Descrição**: Convertendo o DataFrame para RDD, contamos quantos pedidos têm uma quantidade específica de produtos.

---

### **6. Resultados**

- **Resumo dos Resultados**:
  - **Distribuição do Número de Produtos por Pedido**:
    - (8, 211357): 211,357 pedidos com 8 produtos.
    - (32, 8438): 8,438 pedidos com 32 produtos.
    - (24, 28357): 28,357 pedidos com 24 produtos.
    - **Gráfico**: (Insira um gráfico de barras ou histogramas mostrando a distribuição de pedidos por número de produtos.)

- **Insights Obtidos**:
  - **Padrões de Compra**: Identificamos que a maioria dos pedidos tem entre 5 e 10 produtos. Menos pedidos possuem uma quantidade muito alta de produtos, indicando que pedidos maiores são menos comuns.
  - **Análise de Tendências**: A distribuição dos produtos por pedido pode ajudar a entender o comportamento dos clientes e ajustar estratégias de estoque e marketing.

- **Qualidade dos Dados**:
  - **Dados Inconsistentes**: Foram identificadas algumas inconsistências e valores ausentes nos datasets. A limpeza de dados é necessária para melhorar a precisão dos resultados.

---

### **7. Conclusão**

- **Conclusão**: O projeto demonstrou com sucesso como utilizar Apache Spark e Scala para processar grandes volumes de dados e realizar análises complexas, oferecendo insights valiosos sobre o comportamento dos clientes e padrões de compra.

---

### **8. Referências e Agradecimentos**

- **Referências**:
  - Documentação do Apache Spark
  - Tutoriais e exemplos de Scala com Spark
