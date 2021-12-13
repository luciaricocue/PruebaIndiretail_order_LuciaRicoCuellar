#PASO0 opcionA (descartada por inviable): 
#Trabajar con pandas. Es posible leer los .parquet con pandas, pero no hay capacidad para generar el DataFrame daily_stock

#PASO0 opcionB (posible y viable): 
#Instalo Spark 3.2.0 y Hadoop 3.2 y trabajo con visual studio code
#Con esta opcion tengo que instalar las bibliotecas requeridas

#PASO0 opcionC (posible, elegida y viable): 
#Instalo docker en mi PC, descargo imagen jupyter/pyspark-notebook y levanto contenedor
#https://docs.docker.com/desktop/windows/install/
#docker pull jupyter/pyspark-notebook
#docker run -p 8888:8888 -v C:/Users/Lucía/Desktop/PRACTICA_PYTHON/docker_practica_pyspark:/home/jovyan jupyter/pyspark-notebook
#Con esta opción tenemos instaladas las bibliotecas requeridas


#PASO1: Cargo bibliotecas, creo una funcion para saber el numero de filas y columnas de un DataFrame, levanto una sesión de Spark y leo datos de los 3 ficheros .parquet con pyspark
from numpy import prod
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum,max,min,count, lit, when, col, lead, date_sub, explode, sequence, datediff, date_add, year
import sys, os
from pyspark.sql.window import Window
from datetime import datetime


def spark_shape(self):
    return (self.count(), len(self.columns))
pyspark.sql.dataframe.DataFrame.shape = spark_shape


spark = SparkSession.builder.appName('PruebaSpark').getOrCreate()
spark

products=spark.read.parquet("../dat/products.parquet")
sales=spark.read.parquet("../dat/sales.parquet")
stock_movements=spark.read.parquet("../dat/stock_movements.parquet")


#PASO2A: Limpieza de los datos (solo es necesario hacerlo una vez):
#Inspecciono los datos para ver un trozo de tabla (show), 
#esquema (printSchema), 
#dimensión de los datos (filas, columnas con función creada shape), 
#distribución estadística (describe())
#y comprobar que los tipos de datos son los esperados, especialmente, que las fechas tienen el formato correcto y los rangos en los que se distribuyen

# products.show(truncate=False, n=2)
# sales.show(truncate=False, n=2)
# stock_movements.show(truncate=False, n=2)

# products.printSchema()
# sales.printSchema() #fechas con formatos date
# stock_movements.printSchema() #fechas con formatos date

# print("size products antes de quitar NAs:", products.shape())
# print("size sales antes de quitar NAs::", sales.shape())
# print("size stock_movements antes de quitar NAs::", stock_movements.shape())

# print(products.describe().show())
# print(sales.describe().show())
# print(stock_movements.describe().show())

# min_date_sales, max_date_sales = sales.select(min("Date"), max("Date")).first()
# min_date_stock_movements, max_date_stock_movements = stock_movements.select(min("Date"), max("Date")).first()
# print("rango fechas sales", (min_date_sales, max_date_sales))
# print("rango fechas stock movement", (min_date_stock_movements, max_date_stock_movements)) 


#PASO2B: Limpieza de los datos
#Comprobamos si hay filas que contengan NAs o Nulls de las 3 tablas
#Vemos que solo tiene NAs los products en la columna RetailPrices. Habría que pedir a la empresa que nos facilitasen estos 45 ReailPrices (contenidos en el DataFrame products_NA) si quisiéramos tener en cuenta esos productos.
#Vamos a quitar las filas que contengan NAs o Nulls de las 3 tablas, ya que impactarían en los resultados posteriormente (dejamos las 3 lineas para que puedan procesarse comprobando esto igualmente otros ficheros con datos de otras fechas)
#Esto simplifica las comprobaciones posteriores de NAs y Nulls en todos los cálculos.
#Aunque no es necesario, se ha aprovecvhado para odenar las tablas y así poder hacer comprobaciones visuales más facilmente durante el desarrollo

#Productos a revisar por contener NA
#Movimientos de Stock a revisar por contener NA
#Movimientos de Ventas a revisar por contener NA
products_NA=products.where((col("ProductRootCode").isNull())|(col("ProductId").isNull())|(col("Family").isNull())|(col("SupplierPrice").isNull())|(col("RetailPrice").isNull()))
sales_NA=sales.where((col("StoreId").isNull())|(col("ProductId").isNull())|(col("Date").isNull())|(col("Quantity").isNull()))
stock_movements_NA=stock_movements.where((col("StoreId").isNull())|(col("ProductId").isNull())|(col("Date").isNull())|(col("Quantity").isNull()))

# print("size products_NA:", products_NA.shape())
# print("size sales sales_NA:", sales_NA.shape())
# print("size stock_movements_NA :", stock_movements_NA.shape())



products=products.na.drop().orderBy('ProductRootCode', 'ProductId')
sales=sales.na.drop().orderBy( 'StoreId', 'ProductId' , 'Date')
stock_movements=stock_movements.na.drop().orderBy('StoreId', 'ProductId' , 'Date')

# print("size products despues de quitar NAs:", products.shape())
# print("size sales despues de quitar NAs:", sales.shape())
# print("size stock_movements despues de quitar NAs:", stock_movements.shape())

#PASO3 Cálculo del stock diario

# Voy a generar los .parquet:
# *interval_stock.parquet* (Cálculo del stock diario)
# *daily_stock.parquet* (Cálculo del stock diario)
#Vamos a ir haciendo paso a paso lo que indica el enunciado del REDME del ## Daily stock calculation

#A.Añado a stock_movements el ProductRootCode y quito del resultado aquellas filas con productos sin referencia a la tabla products (que no hay ninguno)
df1=stock_movements.join(products.select(products.ProductId,products.ProductRootCode),["ProductId"],"left")
df1=df1.na.drop().orderBy('ProductRootCode', 'StoreId', 'Date')

#B. Sumo las unidades sum(Quantity) de df1 por ('ProductRootCode', 'StoreId', 'Date')  
#   Luego ordeno los datos ('ProductRootCode', 'StoreId', 'Date') para hacer la suma acumulada por ('ProductRootCode', 'StoreId') en orden cronológico
# Example: ![image](https://user-images.githubusercontent.com/7293776/138304030-0d1402e6-0936-49f2-ac33-3488274f7447.png)
df3=df1.groupBy('ProductRootCode', 'StoreId', 'Date').agg(sum("Quantity").alias("Quantity")).orderBy('ProductRootCode', 'StoreId', 'Date')

#C. Añado una columna con la suma acumulada de cada ('ProductRootCode', 'StoreId', 'Date')
# Example: ![image](https://user-images.githubusercontent.com/7293776/138304136-1be755d6-599e-44a5-92c9-c53df033c1c3.png)		      
# para la suma acumulada:
df3 = df3.withColumn('Stock', sum(df3.Quantity).over(Window.partitionBy('ProductRootCode', 'StoreId').orderBy('ProductRootCode', 'StoreId', 'Date').rowsBetween(-sys.maxsize, 0)))

#D. Comprobación: 
#df3.select(sum('Quantity')).collect()[0][0]==df1.select(sum('Quantity')).collect()[0][0]
#tabla3.Quantity.sum()== tabla1.Quantity.sum() #Comprobacion
#tabla3.Quantity.sum()== tabla1.Quantity.sum() #Comprobacion
#df3.show(2)
#print("size df3:", df3.shape())

#E. Para calcular el stock inicial en 01-01-2019 y quedarme con eso y los movimientos de stock posteriores al 01-01-2019 se hace lo siguiente:
#Example: ![image](https://user-images.githubusercontent.com/7293776/138304176-18d7babf-bcb1-4e2d-a95b-4ff176838d84.png)

#POR UN LADO
#Voy a coger los stocks anteriores al 2019 (inclusive el 01-01-2019). 
#Luego me quedo con las filas que contengan el Stock que tenga la fecha más alta para cada pareja (ProductRootCode , StoreId).
#Luego cambio la fecha Date por 2019-01-01 y borro columna Max

df4=df3.filter(df3.Date <='2019-01-01')
df4=df4.drop(df4.Quantity)
df4=df4.select('ProductRootCode', 'StoreId', 'Date',  'Stock', max('Date').over(Window.partitionBy('ProductRootCode', 'StoreId')).alias('Max')).sort('ProductRootCode', 'StoreId', 'Date')
df5=df4.filter(df4.Date==df4.Max)
# df5.shape()
df5=df5.drop(df5.Max)
df5=df5.withColumn("Date", lit(datetime.strptime('2019-01-01' , '%Y-%m-%d')))
#
#POR OTRO LADO
#Ahora me quedo con los stocks posteriores al 2019
df6=df3.filter(df3.Date >'2019-01-01').sort('ProductRootCode', 'StoreId', 'Date')
df6=df6.drop("Quantity")

#
#FINALMENTE
#Uno ambas tablas
df7=df5.union(df6).sort('ProductRootCode', 'StoreId', 'Date')
#
#F. Detecto los casos que se indican que pueden ser borrados en el DataFrame df7:
#  Solo con stock inicial nulo: ("ProductRootCode","StoreId") con una sola fila  y stock 0 y fecha '2019-01-01'
#  Con todos los stocks negativos: ("ProductRootCode","StoreId") con todos los stocks negativos en todas las filas

df7=df7.select('ProductRootCode', 'StoreId', 'Date', 'Stock', count('ProductRootCode').over(Window.partitionBy("ProductRootCode","StoreId")).alias('CuentaFilas')).sort('ProductRootCode', 'StoreId', 'Date')

df7=df7.withColumn("Quitar", when((col("Stock")==0) & (col('Date')=='2019-01-01') & (col('CuentaFilas')==1), 1).otherwise(0))

df7=df7.withColumn("PositCeroStock", when(col("Stock")>=0, 1).otherwise(0))
df7=df7.select('ProductRootCode', 'StoreId', 'Date', 'Stock', 'CuentaFilas', 'Quitar', 'PositCeroStock', sum('PositCeroStock').over(Window.partitionBy("ProductRootCode","StoreId")).alias('Conservar')).sort('ProductRootCode', 'StoreId', 'Date')
df7_quitar=df7.filter((df7.Quitar==1) | (df7.Conservar==0))


#G. Borro los casos detectados en el punto anterior y las columnas auxiliares y me quedo con las filas que me interesen en df8

df8=df7.filter((df7.Quitar==0) & (df7.Conservar!=0))
df8=df8.drop("CuentaFilas", "Quitar", "PositCeroStock", "Conservar")

#H. Me quedo con los datos anteriores al 31-12-2020 (inclusive) y genero el DataFrame con los intervalos de fechas 
#en las que hay cambio de stock por cada pareja ("ProductRootCode","StoreId")
# https://user-images.githubusercontent.com/7293776/138304233-4681ac6f-9185-429a-a378-a77a5499f876.png
df9=df8.filter(df8.Date<'2021-01-01')
df9 = df9.withColumnRenamed("Date","StartDate")
df9=df9.withColumn("EndDate",lead("StartDate",1).over(Window.partitionBy("ProductRootCode","StoreId").orderBy("ProductRootCode","StoreId")))
df9=df9.withColumn("EndDate", date_sub(df9.EndDate, 1))
df9 = df9.withColumn('EndDate', when(col('EndDate').isNull(), datetime.strptime('2020-12-31' , '%Y-%m-%d')).otherwise(col('EndDate'))).select("ProductRootCode", "StoreId", "StartDate", "EndDate", "Stock")
df9_quitar=df9.filter(df9.Stock<=0)
#Tal como se pide, borro de la tabla los intervalos con stock negativo o 0
interval_stock=df9.filter(df9.Stock>0)
# print("interval_stock: ", interval_stock.show())
# print("interval_stock: ", interval_stock.printSchema())
# print("interval_stock: ", interval_stock.shape())


#Comprobación: para saber cuantas filas tienen que salir en daily_stock: calculo los dias de cada intervalo y sumo la columna
# interval_stock=interval_stock.withColumn("diff", datediff(interval_stock.EndDate, interval_stock.StartDate))
# interval_stock=interval_stock.withColumn("diff", interval_stock.diff + 1)
# total_filas=interval_stock.select(sum('diff')).collect()[0][0]
# total_filas
# interval_stock.drop("diff")

# I. Genero todos los días existentes entre el StartDate y el EndDate de cada intervalo de la tabla interval_stock 
# https://user-images.githubusercontent.com/7293776/138304276-59385a4c-f70a-4fa5-8f26-162b7b875160.png
daily_stock = (interval_stock.select('ProductRootCode', 'StoreId', explode(sequence("StartDate","EndDate")).alias("Date"),"Stock"))
# print("daily_stock: ", daily_stock.show())
# print("daily_stock: ", daily_stock.printSchema())
# print("daily_stock: ", daily_stock.shape())

# J. Guardo interval_stock y daily_stock como 2 ficheros .parquet: lo pongo asi para que al volverse a ejecutar sobreescriba lo que hubiera, en caso de que hubiera algo, y si no pues genere la carpeta con el .parquet
# interval_stock.write.format("parquet").mode("overwrite").save("interval_stock.parquet")
# daily_stock.format("parquet").mode("overwrite").save("daily_stock.parquet")

# J. Guardo interval_stock y daily_stock como 2 ficheros .parquet: lo pongo asi para que al volverse a ejecutar sobreescriba lo que hubiera, en caso de que hubiera algo, y si no pues genere la carpeta con el .parquet
interval_stock.write.format('parquet').mode("overwrite").save("../res/interval_stock.parquet")
daily_stock.write.format('parquet').mode("overwrite").save("../res/daily_stock.parquet")

# PASO4: Cálculo del PRIMER KPI: Calculo Profit 
# Profit=(UnitsSold * RetailPrice) - (Inbounds * ProductCost)
# Genero 2 ficheros parquet family_benefits y store_benefits


#PASO4.1: Calculo la primera parte del KPI Profit (UnitsSold * RetailPrice)
#A. Filtro los datos de sales de 2019 y 2020 con Quality positiva y les añado la informacion de Familia y Retail Price
df10=sales.filter((sales.Date>='2019-01-01') & (sales.Date <'2021-01-01') & (sales.Quantity > 0)).join(products.select(products.ProductId,products.Family,products.RetailPrice),["ProductId"],"left")
# print("datos de sales de 2019 y 2010 con Quality postivia: ", df10.shape())


#Como vimos al principio, en sales hay productos que no están referenciados ok en products porque les falta el RetailPrice en este caso
#79262 filas vamos a quitar: 
sales_sin_registroOK_de_producto=df10.where((col("Family").isNull())&(col("RetailPrice").isNull()))
#sales_sin_registroOK_de_producto.show(2)
# print("sales sin registro OK de producto: " ,sales_sin_registroOK_de_producto.shape())


#Quito las filas sin precio (las quito porque no tienen la informacion de RetailPrice, aunque en esta parte necesitamos el SupplierPrice que si que está, pero luego al calcular el KPI completo si que afectaría )
df10=df10.na.drop().orderBy( 'Family','Date', 'ProductId')
# print("sales 2019 y 2020 quitando productos sin registro OK de producto: " ,df10.shape())
#df13.show()



#B. Agego columna de year y columna multiplicando ventas por precio venta
df11=df10.withColumn("Year", year(df10.Date)).withColumn("SalesxRetailPrice", df10.Quantity * df10.RetailPrice).orderBy( 'StoreId', 'Date', 'ProductId')

#C. Agrupo ventas por year y familia y agrupo ventas por year y almacen
df12a=df11.groupBy('Year', 'Family').agg(sum("SalesxRetailPrice").alias("SalesxRetailPrice")).orderBy('Year', 'Family')
df12b=df11.groupBy('Year', 'StoreId').agg(sum("SalesxRetailPrice").alias("SalesxRetailPrice")).orderBy('Year', 'StoreId')

#df11.show(15)
#print(df11.shape())
# print("ventas por year, familia: ", df12a.shape())
#df12a.show()
# print("ventas por year, almacen: ",df12b.shape())
#df12b.show()



##PASO4.2:Calculo la segunda parte del KPI Profit (Inbounds * ProductCost)
#A. Filtro los datos de stock_movements de 2019 y 2020 con Quality postivia y les añado la informacion de Familia y Retail Price
df13=stock_movements.filter((stock_movements.Date>='2019-01-01') & (stock_movements.Date <'2021-01-01') & (stock_movements.Quantity > 0)).join(products.select(products.ProductId,products.Family,products.SupplierPrice),["ProductId"],"left")
# print("datos de stock_movements de 2019 y 2010 con Quality postivia: ", df13.shape())

##Como vimos al principio, en stock_movements realmente no había productos sin estar referenciados en la tabla de los productos, pero al haber quitado los que no tenían RetailPrice, tampoco los consideramos en esta parte aunque si que tengan el SupplierPrice
#11311 filas vamos a quitar
stock_movements_sin_registroOK_de_producto=df13.where((col("Family").isNull())&(col("SupplierPrice").isNull()))
#stock_movements_sin_registroOK_de_producto.show(2)
# print("stock_movements sin registro OK de producto: " ,stock_movements_sin_registroOK_de_producto.shape())

#Quito las filas sin precio (las quito porque no tienen la informacion de RetailPrice, aunque en esta parte necesitamos el SupplierPrice que si que está, pero luego al calcular el KPI completo si que afectaría )
df13=df13.na.drop().orderBy( 'StoreId','Date', 'ProductId')
# print("stock_movements 2019 y 2020 quitando productos sin registro OK de producto: " ,df13.shape())
#df13.show()

#B. Agego columna de year y columna multiplicando ventas por precio venta
df14=df13.withColumn("Year", year(df13.Date)).withColumn("Stock_movementsxSupplierPrice", df13.Quantity * df13.SupplierPrice).orderBy( 'StoreId', 'Date', 'ProductId')

#C. Agrupo ventas por year y familia y agrupo ventas por year y almacen
df15a=df14.groupBy('Year', 'Family').agg(sum("Stock_movementsxSupplierPrice").alias("Stock_movementsxSupplierPrice")).orderBy('Year', 'Family')
df15b=df14.groupBy('Year', 'StoreId').agg(sum("Stock_movementsxSupplierPrice").alias("Stock_movementsxSupplierPrice")).orderBy('Year', 'StoreId')

#df14.show(15)
#print(df14.shape())
# print("stock_movements por year, familia: ", df15a.shape())
#df15a.show()
# print("stock_movements por year, almacen: ",df15b.shape())
#df15b.show()



##PASO4.3:
#Termino el cálculo del KPI Profit para family_benefits: Hago la resta (UnitsSold * RetailPrice) - (Inbounds * ProductCost)
family_benefits=df12a.join(df15a,['Year', 'Family'],"full").sort('Year', 'Family', 'SalesxRetailPrice')
# family_benefits.show()
#Aunque no hay para este caso, si hubiera algún NA-Null, se cambia por 0 (implicaría que no ha habido venta o stock de esa Familia de productos en ningún almacén)
family_benefits=family_benefits.na.fill(value=0)
# print(family_benefits.shape())
# family_benefits.show()
family_benefits=family_benefits.withColumn("Profit", family_benefits.SalesxRetailPrice-family_benefits.Stock_movementsxSupplierPrice).orderBy( 'Year', 'Family')
family_benefits=family_benefits.drop("SalesxRetailPrice", "Stock_movementsxSupplierPrice").orderBy( 'Year', 'Family')
# family_benefits.show()
# print(family_benefits.shape())

##PASO4.4:
#Termino el cálculo del KPI Profit para store_benefits: Hago la resta (UnitsSold * RetailPrice) - (Inbounds * ProductCost)
store_benefits=df12b.join(df15b,['Year', 'StoreId'],"full").sort('Year', 'StoreId', 'SalesxRetailPrice')
# store_benefits.show()
#En este caso si que hay Stores que no ha tenido ventas registradas, los NA-Null se cambian por 0 para hacer la resta
store_benefits=store_benefits.na.fill(value=0)
# print(store_benefits.shape())
# store_benefits.show()

store_benefits=store_benefits.withColumn("Profit", store_benefits.SalesxRetailPrice-store_benefits.Stock_movementsxSupplierPrice).orderBy( 'Year', 'StoreId')
store_benefits=store_benefits.drop("SalesxRetailPrice", "Stock_movementsxSupplierPrice").orderBy( 'Year', 'StoreId')
# store_benefits.show()
# print(store_benefits.shape())


##PASO4.5:Guardo los .parquet
family_benefits.write.format('parquet').mode("overwrite").save("../res/family_benefits.parquet")
store_benefits.write.format('parquet').mode("overwrite").save("../res/store_benefits.parquet")

# PASO5: Cálculo del SEGUNDO KPI: Calculo Turnover 
# Turnover=UnitsSold(during the period)/AverageStock(in the period)
# Genero 2 ficheros parquet family_rotation y store_rotation



#PASO5.1: Calculo la primera parte del KPI Turnover (el numerador): UnitsSold(during the period)
#A. Uso el d10 calculado antes: Filtro los datos de sales de 2019 y 2020 con Quality positiva y les añado la informacion de Familia y Retail Price
#Es el df10 que he calculado antes
# df10.show()
# df10.shape()

#B. Agego columna de year y columna sumando todas las unidadess vendidas
df11_kpi2=df10.withColumn("Year", year(df10.Date)).orderBy( 'Family', 'Date', 'ProductId')
# df11_kpi2.show()
# df11_kpi2.shape()
#C. Agrupo ventas por year y familia y agrupo ventas por year y almacen
df12a_kpi2=df11_kpi2.groupBy('Year', 'Family').agg(sum("Quantity").alias("UnitsSold")).orderBy('Year', 'Family')
df12b_kpi2=df11_kpi2.groupBy('Year', 'StoreId').agg(sum("Quantity").alias("UnitsSold")).orderBy('Year', 'StoreId')

# df11_kpi2.show(15)
# print(df11_kpi2.shape())
# print("ventas por year, familia: ", df12a_kpi2.shape())
# df12a_kpi2.show()
# print("ventas por year, almacen: ",df12b_kpi2.shape())
# df12b_kpi2.show()


##PASO5.2:Calculo la segunda parte del KPI Turnover(denominador): AverageStock(in the period)
# NOTA1: Esta parte NO podemos hacerla con el DataFrame calculado anteriormente daily_stock. 
# Esto es debido a que el fichero daily_stock estaba calculado por ProductRootCode y StoreId
# y a un mismo ProductRootCode pueden corresponderle varias Familias, dependiendo del producto, por lo que no es posible asignar una única familia a los ProductRootCode, 
# y al hacer el join las filas se van a duplicar en los caso en los que pase esto.
#Vamos a recalcular daily_stock_v2 para familias en vez de ProductRootCode
# NOTA2:Este fichero no tiene por qué incluir todos los días del año para todas las Familias y Almacenes ya que en periodos de stock negativo o 0 no hay fila,
# por lo que el stock promedio lo vamos a calcular sumando todos y dividiendo entre los días del año
# 2019 365 días, pero 2020 366 porque fue bisiesto

#A.Recalculamos stock diario
####################################################################################
###################################################################################
#A.Añado a stock_movements el Family y quito del resultado aquellas filas con productos sin referencia a la tabla products (que no hay ninguno)
df1_v2=stock_movements.join(products.select(products.ProductId,products.Family),["ProductId"],"left")
df1_v2=df1_v2.na.drop().orderBy('Family', 'StoreId', 'Date')
#B. Sumo las unidades sum(Quantity) de df1_v2 por ('Family', 'StoreId', 'Date')  
df3_v2=df1_v2.groupBy('Family', 'StoreId', 'Date').agg(sum("Quantity").alias("Quantity")).orderBy('Family', 'StoreId', 'Date')

#C. Añado una columna con la suma acumulada de cada ('Family', 'StoreId', 'Date')
# Example: ![image](https://user-images.githubusercontent.com/7293776/138304136-1be755d6-599e-44a5-92c9-c53df033c1c3.png)		      
# para la suma acumulada:
df3_v2 = df3_v2.withColumn('Stock', sum(df3_v2.Quantity).over(Window.partitionBy('Family', 'StoreId').orderBy('Family', 'StoreId', 'Date').rowsBetween(-sys.maxsize, 0)))

#D. Comprobación: 
#df3_v2.select(sum('Quantity')).collect()[0][0]==df1_v2.select(sum('Quantity')).collect()[0][0]
#df3_v2.select(sum('Quantity')).collect()[0][0]==df1_v2.select(sum('Quantity')).collect()[0][0]
#df3_v2.show(2)
#print("size df3_v2:", df3_v2.shape())

#E. Para calcular el stock inicial en 01-01-2019 y quedarme con eso y los movimientos de stock posteriores al 01-01-2019 se hace lo siguiente:
#Example: ![image](https://user-images.githubusercontent.com/7293776/138304176-18d7babf-bcb1-4e2d-a95b-4ff176838d84.png)

#POR UN LADO
#Voy a coger los stocks anteriores al 2019 (inclusive el 01-01-2019). 
#Luego me quedo con las filas que contengan el Stock que tenga la fecha más alta para cada pareja (Family , StoreId).
#Luego cambio la fecha Date por 2019-01-01 y borro columna Max

df4_v2=df3_v2.filter(df3_v2.Date <='2019-01-01')
df4_v2=df4_v2.drop(df4_v2.Quantity)
df4_v2=df4_v2.select('Family', 'StoreId', 'Date',  'Stock', max('Date').over(Window.partitionBy('Family', 'StoreId')).alias('Max')).sort('Family', 'StoreId', 'Date')
df5_v2=df4_v2.filter(df4_v2.Date==df4_v2.Max)
# df5_v2.shape()
df5_v2=df5_v2.drop(df5_v2.Max)
df5_v2=df5_v2.withColumn("Date", lit(datetime.strptime('2019-01-01' , '%Y-%m-%d')))
#
#POR OTRO LADO
#Ahora me quedo con los stocks posteriores al 2019
df6_v2=df3_v2.filter(df3_v2.Date >'2019-01-01').sort('Family', 'StoreId', 'Date')
df6_v2=df6_v2.drop("Quantity")

#
#FINALMENTE
#Uno ambas tablas
df7_v2=df5_v2.union(df6_v2).sort('Family', 'StoreId', 'Date')
#
#F. Detecto los casos que se indican que pueden ser borrados en el DataFrame df7_v2:
#  Solo con stock inicial nulo: ("Family","StoreId") con una sola fila  y stock 0 y fecha '2019-01-01'
#  Con todos los stocks negativos: ("Family","StoreId") con todos los stocks negativos en todas las filas

df7_v2=df7_v2.select('Family', 'StoreId', 'Date', 'Stock', count('Family').over(Window.partitionBy("Family","StoreId")).alias('CuentaFilas')).sort('Family', 'StoreId', 'Date')

df7_v2=df7_v2.withColumn("Quitar", when((col("Stock")==0) & (col('Date')=='2019-01-01') & (col('CuentaFilas')==1), 1).otherwise(0))

df7_v2=df7_v2.withColumn("PositCeroStock", when(col("Stock")>=0, 1).otherwise(0))
df7_v2=df7_v2.select('Family', 'StoreId', 'Date', 'Stock', 'CuentaFilas', 'Quitar', 'PositCeroStock', sum('PositCeroStock').over(Window.partitionBy("Family","StoreId")).alias('Conservar')).sort('Family', 'StoreId', 'Date')
df7_v2_quitar_v2=df7_v2.filter((df7_v2.Quitar==1) | (df7_v2.Conservar==0))


#G. Borro los casos detectados en el punto anterior y las columnas auxiliares y me quedo con las filas que me interesen en df8_v2

df8_v2=df7_v2.filter((df7_v2.Quitar==0) & (df7_v2.Conservar!=0))
df8_v2=df8_v2.drop("CuentaFilas", "Quitar", "PositCeroStock", "Conservar")

#H. Me quedo con los datos anteriores al 31-12-2020 (inclusive) y genero el DataFrame con los intervalos de fechas 
#en las que hay cambio de stock por cada pareja ("Family","StoreId")
# https://user-images.githubusercontent.com/7293776/138304233-4681ac6f-9185-429a-a378-a77a5499f876.png
df9_v2=df8_v2.filter(df8_v2.Date<'2021-01-01')
df9_v2 = df9_v2.withColumnRenamed("Date","StartDate")
df9_v2=df9_v2.withColumn("EndDate",lead("StartDate",1).over(Window.partitionBy("Family","StoreId").orderBy("Family","StoreId")))
df9_v2=df9_v2.withColumn("EndDate", date_sub(df9_v2.EndDate, 1))
df9_v2 = df9_v2.withColumn('EndDate', when(col('EndDate').isNull(), datetime.strptime('2020-12-31' , '%Y-%m-%d')).otherwise(col('EndDate'))).select("Family", "StoreId", "StartDate", "EndDate", "Stock")
df9_v2_quitar_v2=df9_v2.filter(df9_v2.Stock<=0)
#Tal como se pide, borro de la tabla los intervalos con stock negativo o 0
interval_stock_v2=df9_v2.filter(df9_v2.Stock>0)
# print("interval_stock_v2: ", interval_stock_v2.show())
# print("interval_stock_v2: ", interval_stock_v2.printSchema())
# print("interval_stock_v2: ", interval_stock_v2.shape())


#Comprobación: para saber cuantas filas tienen que salir en daily_stock_v2: calculo los dias de cada intervalo y sumo la columna
# interval_stock_v2=interval_stock_v2.withColumn("diff", datediff(interval_stock_v2.EndDate, interval_stock_v2.StartDate))
# interval_stock_v2=interval_stock_v2.withColumn("diff", interval_stock_v2.diff + 1)
# total_filas=interval_stock_v2.select(sum('diff')).collect()[0][0]
# total_filas
# interval_stock_v2.drop("diff")

# I. Genero todos los días existentes entre el StartDate y el EndDate de cada intervalo de la tabla interval_stock_v2 
# https://user-images.githubusercontent.com/7293776/138304276-59385a4c-f70a-4fa5-8f26-162b7b875160.png
daily_stock_v2 = (interval_stock_v2.select('Family', 'StoreId', explode(sequence("StartDate","EndDate")).alias("Date"),"Stock"))
# print("daily_stock_v2: ", daily_stock_v2.show())
# print("daily_stock_v2: ", daily_stock_v2.printSchema())
# print("daily_stock_v2: ", daily_stock_v2.shape())
####################################################################################
###################################################################################



# #estas 4 iguales
# print(df3.select(sum('Quantity')).collect()[0][0])
# print(df1.select(sum('Quantity')).collect()[0][0])
# print(df3_v2.select(sum('Quantity')).collect()[0][0])
# print(df1_v2.select(sum('Quantity')).collect()[0][0])
# #estas 2 diferentes:
# print(daily_stock.select(sum('Stock')).collect()[0][0])
# print(daily_stock_v2.select(sum('Stock')).collect()[0][0])

#B. daily_stock contiene ya datos del 2019 y 2020 con Quality positiva y Familia


#C. Agego columna de year 
daily_stock_v2=daily_stock_v2.withColumn("Year", year(daily_stock_v2.Date))
# daily_stock_v2.show()
# daily_stock_v2.shape()

#D. Agrupo ventas por year y familia y agrupo ventas por year y almacen
df15a_kpi2=daily_stock_v2.groupBy('Year', 'Family').agg(sum("Stock").alias("SumStock")).orderBy('Year', 'Family')
df15b_kpi2=daily_stock_v2.groupBy('Year', 'StoreId').agg(sum("Stock").alias("SumStock")).orderBy('Year', 'StoreId')

#E. Agrego la columna DaysInTheYear para tener en cuenta periodos sin stock en el promedio
df15a_kpi2=df15a_kpi2.withColumn('DaysInTheYear', when((col("Year")==2020) , 366).otherwise(365)).sort(df15a_kpi2.Year.asc())
df15b_kpi2=df15b_kpi2.withColumn('DaysInTheYear', when((col("Year")==2020) , 366).otherwise(365)).sort(df15b_kpi2.Year.asc())


# df15a_kpi2.show(16)
# df15b_kpi2.show(68)
#print(df14.shape())
# print("daily_stock por year, familia: ", df15a_kpi2.shape())
#df15a.show()
# print("daily_stock por year, almacen: ",df15b_kpi2.shape())
#df15b.show()

#F. Calculo AverageStock dividiendo la suma de stocks entre el total de días en el año
df15a_kpi2=df15a_kpi2.withColumn("AverageStock", (df15a_kpi2.SumStock / df15a_kpi2.DaysInTheYear)).select("Year",  "Family", "AverageStock")
df15b_kpi2=df15b_kpi2.withColumn("AverageStock", (df15b_kpi2.SumStock / df15b_kpi2.DaysInTheYear)).select("Year",  "StoreId", "AverageStock")

##PASO5.3:
#Termino el cálculo del KPI Turnover para family_rotation: Hago la división
family_rotation=df12a_kpi2.join(df15a_kpi2,['Year', 'Family'],"full").sort('Year', 'Family')
# family_rotation.show()
#Aunque no hay para este caso, si hubiera algún NA-Null, se cambia por 0 (implicaría que no ha habido venta o stock de esa Familia de productos en ningún almacén)
family_rotation=family_rotation.na.fill(value=0)
# print(family_rotation.shape())
# family_rotation.show()
family_rotation=family_rotation.withColumn("Turnover", family_rotation.UnitsSold/family_rotation.AverageStock).orderBy( 'Year', 'Family')
family_rotation=family_rotation.drop("UnitsSold", "AverageStock").orderBy( 'Year', 'Family')
# family_rotation.show()
# print(family_rotation.shape())

##PASO5.4:
#Termino el cálculo del KPI Turnover para store_rotation: Hago la división
store_rotation=df12b_kpi2.join(df15b_kpi2,['Year', 'StoreId'],"full").sort('Year', 'StoreId')
# store_rotation.show()
#En este caso si que hay algún NA-Null, se cambia por 0 (implicaría que no ha habido venta o stock de esa Familia de productos en ningún almacén)
store_rotation=store_rotation.na.fill(value=0)
# print(store_rotation.shape())
# store_rotation.show()
store_rotation=store_rotation.withColumn("Turnover", store_rotation.UnitsSold/store_rotation.AverageStock).orderBy( 'Year', 'StoreId')
store_rotation=store_rotation.drop("UnitsSold", "AverageStock").orderBy( 'Year', 'StoreId')
# store_rotation.show()
# print(store_rotation.shape())

##PASO5.5:Guardo los .parquet
family_rotation.write.format('parquet').mode("overwrite").save("../res/family_rotation.parquet")
store_rotation.write.format('parquet').mode("overwrite").save("../res/store_rotation.parquet")