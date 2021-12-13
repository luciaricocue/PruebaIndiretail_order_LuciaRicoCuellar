# PruebaIndiretail_order_LuciaRicoCuellar
# Prueba técnica de ingeniero de datos (stock)

#Contenido:
Código a ejecutar: /src/PruebaIndiretail_order.py
Datos proporcionados: /dat/*
Datos de obtenidos: /res/*

# Objetivo:
A partir de 3 .parquet dados, leer, hacer cálculos y transformaciones y generar los 6 ficheros .parquet requeridos en la prueba:

Estos son los 3 ficheros .parquet dados (dentro de la carpeta dat/):

*stock_movements.parquet*
*sales.parquet*
*products.parquet*

Estos son los 6 ficheros a generar (dentro de la carpeta res/):

*interval_stock.parquet* (Cálculo del stock diario)
*daily_stock.parquet* (Cálculo del stock diario)
*store_benefits.parquet* (KPIs)
*family_benefits.parquet* (KPIs)
*store_rotation.parquet* (KPIs)
*family_rotation.parquet* (KPIs)


Los 3 ficheros dados contienen esta información:

# Movimientos de stock
*stock_movements.parquet* contiene esta información:

- StoreId (integer): store code.
- ProductId (integer): product code.
- Date (date): inbound/outbound date.
- Quantity (integer): inbound (+) or outbound (-).

# Movimientos de ventas
*sales.parquet* contiene esta información:

- StoreId (integer): store code.
- ProductId (integer): product code.
- Date (date): date of sale.
- Quantity (integer): units sold (negative values are returns).

# Identificación de los productos
*products.parquet* contiene esta información:

- ProductRootCode (integer): product root code, without size.
- ProductId (integer): product code.
- SupplierPrice (float): product cost.
- RetailPrice (float): retail price of the product.
- Family (string): product family to which it belongs.


Los 6 ficheros generados contendrán esta información:

# *interval_stock.parquet* (Cálculo del stock diario)
root
 |-- ProductRootCode: integer (nullable = true)
 |-- StoreId: integer (nullable = true)
 |-- StartDate: timestamp (nullable = true)
 |-- EndDate: timestamp (nullable = true)
 |-- Stock: long (nullable = true)

# *daily_stock.parquet* (Cálculo del stock diario)
root
 |-- ProductRootCode: integer (nullable = true)
 |-- StoreId: integer (nullable = true)
 |-- Date: timestamp (nullable = false)
 |-- Stock: long (nullable = true)

# *store_benefits.parquet* (KPIs)
root
 |-- Year: integer (nullable = true)
 |-- StoreId: integer (nullable = true)
 |-- Profit: double (nullable = false)

# *family_benefits.parquet* (KPIs)
root
 |-- Year: integer (nullable = true)
 |-- Family: string (nullable = true)
 |-- Profit: double (nullable = false)

# *store_rotation.parquet* (KPIs)
root
 |-- Year: integer (nullable = true)
 |-- StoreId: integer (nullable = true)
 |-- Turnover: double (nullable = true)

# *family_rotation.parquet* (KPIs)
root
 |-- Year: integer (nullable = true)
 |-- Family: string (nullable = true)
 |-- Turnover: double (nullable = true)


# PASOS SEGUIDOS (se explica sobre el código con más detalle, aquí se ha indicado lo más relevante)

#PASO0: Elección de Herramientas
#PASO0 opcionA (descartada por inviable): 
 Trabajar con pandas. Es posible leer los .parquet con pandas, pero no hay capacidad para generar el DataFrame daily_stock

#PASO0 opcionB (posible y viable): 
 Instalo Spark 3.2.0 y Hadoop 3.2 y trabajo con visual studio code
 Con esta opcion tengo que instalar las bibliotecas requeridas

#PASO0 opcionC (posible, elegida y viable): 
 Instalo docker en mi PC, descargo imagen jupyter/pyspark-notebook y levanto contenedor
 https://docs.docker.com/desktop/windows/install/
 docker pull jupyter/pyspark-notebook
 docker run -p 8888:8888 -v C:/Users/Lucía/Desktop/PRACTICA_PYTHON/docker_practica_pyspark:/home/jovyan jupyter/pyspark-notebook
 Con esta opción tenemos instaladas las bibliotecas requeridas


#PASO1: Cargo bibliotecas, creo una funcion para saber el numero de filas y columnas de un DataFrame, levanto una sesión de Spark y leo datos de los 3 ficheros .parquet con pyspark

#PASO2A: Limpieza de los datos (solo es necesario hacerlo una vez)
 Inspecciono los datos para ver un trozo de tabla (show), 
 esquema (printSchema), 
 dimensión de los datos (filas, columnas con función creada shape), 
 distribución estadística (describe())
 y comprobar que los tipos de datos son los esperados, especialmente, que las fechas tienen el formato correcto y los rangos en los que se distribuyen


#PASO2B: Limpieza de los datos
 Comprobamos si hay filas que contengan NAs o Nulls de las 3 tablas y las quitamos
 Vemos que solo tiene NAs los products en la columna RetailPrices. Habría que pedir a la empresa que nos facilitasen estos 45 ReailPrices (contenidos en el DataFrame products_NA) si quisiéramos tener en cuenta esos productos.
 Aunque no es necesario, se ha aprovechado para odenar las tablas y así poder hacer comprobaciones visuales más facilmente durante el desarrollo

#PASO3 Cálculo del stock diario

  Voy a generar los .parquet:
  *interval_stock.parquet* (Cálculo del stock diario)
  *daily_stock.parquet* (Cálculo del stock diario)
 Vamos a ir haciendo paso a paso lo que indica el enunciado del REDME del ## Daily stock calculation
 Más detalle en comentarios del código


# PASO4: Cálculo del PRIMER KPI: Calculo Profit 
  Profit=(UnitsSold * RetailPrice) - (Inbounds * ProductCost)
  Genero 2 ficheros parquet family_benefits y store_benefits
 Se han seguido también tal cual las indicaciones del enunciado

 #PASO4.1: Calculo la primera parte del KPI Profit (UnitsSold * RetailPrice)
 Como vimos al principio, en sales hay productos que no están referenciados ok en products porque les falta el RetailPrice en este caso
 79262 filas vamos a quitar: 

 #PASO4.2:Calculo la segunda parte del KPI Profit (Inbounds * ProductCost)
  Como vimos al principio, en stock_movements realmente no había productos sin estar referenciados en la tabla de los productos, pero al haber quitado los que no tenían RetailPrice, tampoco los consideramos en esta parte aunque si que tengan el SupplierPrice
 11311 filas vamos a quitar

 #PASO4.3:
 Termino el cálculo del KPI Profit para family_benefits: Hago la resta (UnitsSold * RetailPrice) - (Inbounds * ProductCost)
 Aunque no hay para este caso, si hubiera algún NA-Null, se cambia por 0 (implicaría que no ha habido venta o stock de esa Familia de productos en ningún almacén)

 #PASO4.4:
 Termino el cálculo del KPI Profit para store_benefits: Hago la resta (UnitsSold * RetailPrice) - (Inbounds * ProductCost)
 En este caso si que hay Stores que no ha tenido ventas registradas, los NA-Null se cambian por 0 para hacer la resta

 #PASO4.5:Guardo los .parquet

# PASO5: Cálculo del SEGUNDO KPI: Calculo Turnover 
  Turnover=UnitsSold(during the period)/AverageStock(in the period)
  Genero 2 ficheros parquet family_rotation y store_rotation

#PASO5.1: Calculo la primera parte del KPI Turnover (el numerador): UnitsSold(during the period)
#A. Uso el df10 calculado antes:  en el paso 4.1 (Filtro los datos de sales de 2019 y 2020 con Quality positiva y les añado la informacion de Familia y Retail Price)

#PASO5.2:Calculo la segunda parte del KPI Turnover(denominador): AverageStock(in the period)
# NOTA1: Esta parte NO podemos hacerla con el DataFrame calculado anteriormente daily_stock, como se pide en en enunciado. 
  Esto es debido a que el fichero daily_stock estaba calculado por ProductRootCode y StoreId
  y a un mismo ProductRootCode pueden corresponderle varias Familias, dependiendo del producto, por lo que no es posible asignar
  una única familia a cada ProductRootCode, 
  y al hacer el join las filas se van a duplicar en los caso en los que pase esto.
  Vamos a recalcular daily_stock_v2 para familias en vez de ProductRootCode
# NOTA2:Este fichero no tiene por qué incluir todos los días del año para todas las Familias y Almacenes ya que en periodos de 
  stock negativo o 0 no hay fila,
  por lo que el stock promedio lo vamos a calcular sumando todos y dividiendo entre los días del año
  2019 365 días, pero 2020 366 porque fue bisiesto.

#PASO5.3:
 Termino el cálculo del KPI Turnover para family_rotation: Hago la división
 Aunque no hay para este caso, si hubiera algún NA-Null, se cambia por 0 (implicaría que no ha habido venta o stock de esa Familia de productos en ningún almacén)

#PASO5.4:
 Termino el cálculo del KPI Turnover para store_rotation: Hago la división
 En este caso si que hay algún NA-Null, se cambia por 0 (implicaría que no ha habido venta o stock de esa Familia de productos en ningún almacén)






