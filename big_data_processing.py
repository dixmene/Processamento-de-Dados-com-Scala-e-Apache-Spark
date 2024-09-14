from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

# 1. Configurar ambiente Spark
spark = SparkSession.builder.appName("BigDataProcessing").getOrCreate()
print(f"Spark Version: {spark.version}")

# 2. Carregar os datasets
try:
    aisles = spark.read.csv('/mnt/d/Downloads/Instacart Market Basket Analysis/aisles.csv', header=True, inferSchema=True)
    departments = spark.read.csv('/mnt/d/Downloads/Instacart Market Basket Analysis/departments.csv', header=True, inferSchema=True)
    order_products_prior = spark.read.csv('/mnt/d/Downloads/Instacart Market Basket Analysis/order_products__prior.csv', header=True, inferSchema=True)
    order_products_train = spark.read.csv('/mnt/d/Downloads/Instacart Market Basket Analysis/order_products__train.csv', header=True, inferSchema=True)
    orders = spark.read.csv('/mnt/d/Downloads/Instacart Market Basket Analysis/orders.csv', header=True, inferSchema=True)
    products = spark.read.csv('/mnt/d/Downloads/Instacart Market Basket Analysis/products.csv', header=True, inferSchema=True)

    # Exibir o esquema dos datasets
    print("Aisles Schema:")
    aisles.printSchema()

    print("Departments Schema:")
    departments.printSchema()

    print("Order Products Prior Schema:")
    order_products_prior.printSchema()

    print("Order Products Train Schema:")
    order_products_train.printSchema()

    print("Orders Schema:")
    orders.printSchema()

    print("Products Schema:")
    products.printSchema()
except Exception as e:
    print(f"Erro ao carregar os dados: {e}")

# 3. Realizar operações de transformação
try:
    order_products_combined = order_products_prior.union(order_products_train)
    order_counts = order_products_combined.groupBy("order_id").agg(count("product_id").alias("product_count"))
    print("Dados transformados:")
    order_counts.show(5)
except Exception as e:
    print(f"Erro na transformação dos dados: {e}")

# 4. Operações de redução e agregação usando RDD
try:
    if 'product_count' in order_counts.columns:
        # Converter DataFrame para RDD
        orders_rdd = order_counts.rdd.map(lambda row: (row['order_id'], row['product_count']))

        # Realizar a agregação: contar o número de pedidos para cada número de produtos
        count_by_product_count = orders_rdd.map(lambda x: (x[1], 1)).reduceByKey(lambda a, b: a + b)

        print("Resultados da agregação:")
        print(count_by_product_count.collect())
    else:
        print("A coluna 'product_count' não foi encontrada no DataFrame 'order_counts'.")
except Exception as e:
    print(f"Erro na agregação dos dados: {e}")

# Encerrar a sessão Spark
spark.stop()
