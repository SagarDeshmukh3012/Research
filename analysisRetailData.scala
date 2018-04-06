import org.apache.spark.rdd.RDD
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ FSDataInputStream, FileSystem, Path }
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.expressions.Window
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ FSDataInputStream, FileSystem, Path }
import org.apache.spark.sql.types.{ StringType, StructField, StructType }
import org.apache.spark.storage.StorageLevel

//Function Used for Creating or defining schemas for Dataframes.
def schemaDef(baseSchema: String): StructType = {
   return StructType(baseSchema.split(",").map(f => StructField(f, StringType, true)))
}

//Reading Input files from Local and Creating RDDs
val Customers: RDD[String] = sc.textFile("file:///d2/data/apps/testing/asgn/Customers.txt")
val Order_Items: RDD[String] = sc.textFile("file:///d2/data/apps/testing/asgn/Order_Items.txt")
val Orders: RDD[String] = sc.textFile("file:///d2/data/apps/testing/asgn/Orders.txt")
val Products: RDD[String] = sc.textFile("file:///d2/data/apps/testing/asgn/Products.txt")

//We have appended show(false) to each query to view sample data while executing this Script.

//Creating Dataframes from RDDs and creating Temporary Tables.
val Customers_bc = sc.broadcast(Customers)
val Customers_schema = "customer_id,customer_fname,customer_lname,customer_email,customer_password,customer_street,customer_city,customer_state,customer_zipcode"
val Customers_s = schemaDef(Customers_schema)
val Customers_row = Customers_bc.value.map(_.split(";")).map(r => org.apache.spark.sql.Row(r(0), r(1), r(2), r(3), r(4), r(5), r(6), r(7), r(8)))
val CustomersDF = sqlContext.createDataFrame(Customers_row, Customers_s)
CustomersDF.distinct.registerTempTable("CustomersDF")
CustomersDF.persist(StorageLevel.MEMORY_AND_DISK)
CustomersDF.show(false)


val Order_Items_bc = sc.broadcast(Order_Items)
val Order_Items_schema = "order_item_id,order_item_order_id,order_item_product_id,order_item_quantity,order_item_subtotal,order_item_product_price"
val Order_Items_s = schemaDef(Order_Items_schema)
val Order_Items_row = Order_Items_bc.value.map(_.split(",")).map(r => org.apache.spark.sql.Row(r(0), r(1), r(2), r(3), r(4), r(5)))
val Order_ItemsDF = sqlContext.createDataFrame(Order_Items_row, Order_Items_s)
Order_ItemsDF.distinct.registerTempTable("Order_ItemsDF")
Order_ItemsDF.persist(StorageLevel.MEMORY_AND_DISK)
Order_ItemsDF.show(false)



val Orders_bc = sc.broadcast(Orders)
val Orders_schema = "order_id,order_date,order_customer_id,order_status"
val Orders_s = schemaDef(Orders_schema)
val Orders_row = Orders_bc.value.map(_.split(",")).map(r => org.apache.spark.sql.Row(r(0), r(1), r(2), r(3)))
val OrdersDF = sqlContext.createDataFrame(Orders_row, Orders_s)
OrdersDF.distinct.registerTempTable("OrdersDF")
OrdersDF.persist(StorageLevel.MEMORY_AND_DISK)
OrdersDF.show(false)


val Products_bc = sc.broadcast(Products)
val Products_schema = "product_id,product_category_id,product_name,product_description,product_price,product_image"
val Products_s = schemaDef(Products_schema)
val Products_row = Products_bc.value.map(_.split(";")).map(r => org.apache.spark.sql.Row(r(0), r(1), r(2), r(3), r(4), r(5)))
val ProductsDF = sqlContext.createDataFrame(Products_row, Products_s)
ProductsDF.distinct.registerTempTable("ProductsDF")
ProductsDF.persist(StorageLevel.MEMORY_AND_DISK)
ProductsDF.show(false)


//1
//We are taking sum of subtotal column from Order_ItemsDF based on order_Date from OrdersDF
val Total_Sale_Each_date_query:String = "select o.order_date,sum(oi.order_item_subtotal) from Order_ItemsDF oi join ordersDF o on oi.order_item_order_id=o.order_id group by o.order_date" 
val Total_Sale_Each_date = sqlContext.sql(Total_Sale_Each_date_query).toDF("Date","TotalSale").persist(StorageLevel.MEMORY_AND_DISK)
Total_Sale_Each_date.registerTempTable("Total_Sale_Each_date")
Total_Sale_Each_date.show(false)

//2
//We are taking sum of subtotal column from Order_ItemsDF based on Year and Month from OrdersDF
val Total_Sale_Each_month_query:String = "select year(o.order_date), date_format(o.order_date,'MMM') ,sum(oi.order_item_subtotal) from Order_ItemsDF oi join OrdersDF o on oi.order_item_order_id=o.order_id group by year(o.order_date), date_format(o.order_date,'MMM')" 
val Total_Sale_Each_month = sqlContext.sql(Total_Sale_Each_month_query).toDF("Year","Month","TotalSale").persist(StorageLevel.MEMORY_AND_DISK)
Total_Sale_Each_month.registerTempTable("Total_Sale_Each_month")
Total_Sale_Each_month.show(false)

//3
// We are taking sum of Total Sale from Total_Sale_Each_month table and divided it by 365 to get avg sale for each daya based on year.
val Avg_Sale_Each_date_query:String = "select Year,(Sum(TSEM.TotalSale)/365) as AvgSalePerDay from Total_Sale_Each_month TSEM group by year" 
val Avg_Sale_Each_date = sqlContext.sql(Avg_Sale_Each_date_query).toDF("Year","AvgSaleEachDay").persist(StorageLevel.MEMORY_AND_DISK)
Avg_Sale_Each_date.registerTempTable("Avg_Sale_Each_date")
Avg_Sale_Each_date.show(false)

//4
// We are taking sum of Total Sale from Total_Sale_Each_month table and divided it by 12 to get avg sale for each month based on year & month.
val Avg_Sale_Each_month_query:String = "select Year,month,(Sum(TSEM.TotalSale)/12) as AvgSaleEachMonth from Total_Sale_Each_month TSEM group by year,month" 
val Avg_Sale_Each_month = sqlContext.sql(Avg_Sale_Each_month_query).toDF("Year","Month","AvgSaleEachMonth").persist(StorageLevel.MEMORY_AND_DISK)
Avg_Sale_Each_month.registerTempTable("Avg_Sale_Each_month")
Avg_Sale_Each_month.show(false)

//5 we have calculated max(TotalSale) from Total_Sale_Each_month table and did a self join to get the respective month of that maxsale.
sqlContext.sql("select a.year,a.month,b.maxsale from Total_Sale_Each_month a join (select max(TotalSale) as maxsale from Total_Sale_Each_month b)b on  a.TotalSale=b.maxsale").show(false)

//6
// We have joined order and order_item DFs to get order_item_product_id for each table.
// We have calculated sum(order_item_quantity) based on order_item_product_id
// We have then Joined above dataset with Products DF to get product Price and multiplied it with total_quantity.
// We have then ranked this dataset based on ProductRevenue and got the top 10 products.
sqlContext.sql("select f.product_name,f.ProductRevenue,f.rnk  from (select p.product_name,(p.product_price*a.total_quantity) as ProductRevenue,dense_rank() over (order by (p.product_price*a.total_quantity) desc) as rnk from ( select oi.order_item_product_id,sum(order_item_quantity) as total_quantity from ordersDF o join order_itemsDF oi on o.order_id=oi.order_item_order_id group by oi.order_item_product_id )a join ProductsDF p on a.order_item_product_id=p.product_id )F where f.rnk<=10").show(false)

//7
// We have joined order and order_item DFs to get order_item_product_id & order_date for each table.
// We have calculated sum(order_item_quantity) as based on order_item_product_id & order_date
// We have then Joined above dataset with Products DF to get product Price and multiplied it with total_quantity to get subtotal.
//We have then calculated sum(subtotal) as Purchase based on order_item_product_id and year and month.
// We have then ranked this dataset based on Purchase wrt year and month and got the top 3 Customers.
sqlContext.sql("select concat_ws(' ',C.customer_fname,C.customer_lname) as Customer_name,z.year,z.month,z.purchase,z.rnk from (select y.order_customer_id,y.year,y.month,y.Purchase,y.rnk from (select x.order_customer_id,x.year,x.month,x.Purchase,dense_rank() over (partition by x.year, x.month order by x.Purchase desc) as rnk  from (select b.order_customer_id,year(b.order_date) as year ,month(b.order_date) as month,sum(b.Subtotal) as Purchase from( select a.order_customer_id,a.order_date,a.order_id,a.order_item_product_id,(p.product_price*a.total_quantity) as Subtotal from (select o.order_customer_id,o.order_date,o.order_id,oi.order_item_product_id,sum(order_item_quantity) as total_quantity from ordersDF o join order_itemsDF oi on o.order_id=oi.order_item_order_id group by o.order_customer_id,o.order_date,o.order_id,oi.order_item_product_id order by o.order_customer_id desc,o.order_date desc,o.order_id desc ,oi.order_item_product_id desc )a join ProductsDF p on a.order_item_product_id=p.product_id )b group by b.order_customer_id,year(b.order_date),month(b.order_date) )x )y where y.rnk<=3 )z join CustomersDF c on z.order_customer_id=c.customer_id").show(false)


//8
// We have joined order and order_item DFs to get order_item_product_id & order_date for each table.
// We have calculated sum(order_item_quantity) as based on order_item_product_id & order_date
// We have Joined above dataset with Products DF and calculated max(order_item_quantity) as maxq based on  order_date and Product_name 
// We have then ranked this dataset based on maxq wrt order_date and got the Most sold Products for each date.

sqlContext.sql("select y.order_date,y.product_name,y.maxq as MostSoldQuanity from( select x.order_date,x.product_name,x.maxq,dense_rank() over (partition by x.order_date order by x.maxq desc) as rnk from ( select a.order_date,p.product_name, max(a.total_quantity) as maxq from ( select o.order_date,oi.order_item_product_id,sum(order_item_quantity) as total_quantity from ordersDF o join order_itemsDF oi on o.order_id=oi.order_item_order_id group by o.order_date,oi.order_item_product_id order by o.order_date desc,oi.order_item_product_id desc )a join ProductsDF p on a.order_item_product_id=p.product_id group by a.order_date,p.product_name order by a.order_date desc,p.product_name desc )x )y where rnk=1").show(false)

//9
// We have taken distinct count of Customers from Customer table based on customer_state
sqlContext.sql("select c.customer_state,count(distinct c.customer_id) CntofCustomers from CustomersDF c group by c.customer_state order by c.customer_state").show(false)

//10
//Maximum Sold Category wise
//Approach 1
// We have joined Products and order_item DFs to get Sum(order_item_quantity) as total_quantity & p.product_category_id  based on product_category_id.
//We have calculated max(total_quantity) as maxq based on product_category_id.
// We have then ranked this dataset based on maxq and got the topmost product Category.
sqlContext.sql("select y.product_category_id,y.maxq from( select x.product_category_id,x.maxq,dense_rank() over (order by x.maxq desc) as rnk from ( select p.product_category_id, max(a.total_quantity) as maxq from ( select distinct oi.order_item_product_id,sum(oi.order_item_quantity) as total_quantity from order_itemsDF oi group by oi.order_item_product_id order by oi.order_item_product_id desc )a join ProductsDF p on a.order_item_product_id=p.product_id group by p.product_category_id )x )y where y.rnk=1").show(100)

//Present in Maximum Order 
//Approach 2
//// We have joined Products and order_item DFs to get count(order_item_order_id) & p.product_category_id for based on product_category_id.
// We have then ranked this dataset based on count(order_item_order_id) and got the topmost product Category. 
sqlContext.sql("select y.product_category_id,y.OrdersCnt from ( select x.product_category_id,x.OrdersCnt, dense_rank() over (order by x.OrdersCnt desc) as rnk from ( select p.product_category_id, count(order_item_order_id) as OrdersCnt from productsDF p join order_itemsDF oi on oi.order_item_product_id=p.product_id group by  p.product_category_id order by OrdersCnt desc )x )y where rnk=1").show(100)

