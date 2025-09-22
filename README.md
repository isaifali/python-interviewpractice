Part D: Window Functions & Ranking (151–200)
Data Setup
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, rank, dense_rank, lead, lag, sum, avg
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("PySparkInterview").getOrCreate()

# Employee DataFrame
emp_data = [
    (1,"Alice",29,"NY","HR",5000),
    (2,"Bob",31,"CA","Engineering",6000),
    (3,"Cathy",25,"TX","Sales",4500),
    (4,"David",35,"NY","HR",7000),
    (5,"Eve",28,"CA","Engineering",5200),
    (6,"Frank",32,"TX","Sales",4800),
    (7,"Grace",30,"NY","HR",5500)
]
emp_columns = ["emp_id","name","age","state","dept_name","salary"]
df_emp = spark.createDataFrame(emp_data, emp_columns)

151. Row number per department ordered by salary descending
windowSpec = Window.partitionBy("dept_name").orderBy(col("salary").desc())
df_emp.withColumn("row_num", row_number().over(windowSpec)).show()

152. Rank per department by salary
windowSpec = Window.partitionBy("dept_name").orderBy(col("salary").desc())
df_emp.withColumn("rank", rank().over(windowSpec)).show()

153. Dense rank per department by salary
windowSpec = Window.partitionBy("dept_name").orderBy(col("salary").desc())
df_emp.withColumn("dense_rank", dense_rank().over(windowSpec)).show()

154. Lead salary by 1 row per department
windowSpec = Window.partitionBy("dept_name").orderBy("salary")
df_emp.withColumn("next_salary", lead("salary",1).over(windowSpec)).show()

155. Lag salary by 1 row per department
windowSpec = Window.partitionBy("dept_name").orderBy("salary")
df_emp.withColumn("prev_salary", lag("salary",1).over(windowSpec)).show()

156. Cumulative sum of salary per department
windowSpec = Window.partitionBy("dept_name").orderBy("salary").rowsBetween(Window.unboundedPreceding, Window.currentRow)
df_emp.withColumn("cum_salary", sum("salary").over(windowSpec)).show()

157. Cumulative average salary per department
windowSpec = Window.partitionBy("dept_name").orderBy("salary").rowsBetween(Window.unboundedPreceding, Window.currentRow)
df_emp.withColumn("cum_avg_salary", avg("salary").over(windowSpec)).show()

158. Top 2 salaries per department
windowSpec = Window.partitionBy("dept_name").orderBy(col("salary").desc())
df_emp.withColumn("rank", row_number().over(windowSpec)).filter(col("rank")<=2).show()

159. Employees with salary difference from previous employee per department
windowSpec = Window.partitionBy("dept_name").orderBy("salary")
df_emp.withColumn("prev_salary", lag("salary",1).over(windowSpec)).withColumn("salary_diff", col("salary")-col("prev_salary")).show()

160. Employees with salary difference from next employee per department
windowSpec = Window.partitionBy("dept_name").orderBy("salary")
df_emp.withColumn("next_salary", lead("salary",1).over(windowSpec)).withColumn("salary_diff_next", col("next_salary")-col("salary")).show()

161. Total salary per department (window function)
windowSpec = Window.partitionBy("dept_name")
df_emp.withColumn("total_salary_dept", sum("salary").over(windowSpec)).show()

162. Average age per department
windowSpec = Window.partitionBy("dept_name")
df_emp.withColumn("avg_age_dept", avg("age").over(windowSpec)).show()

163. Add dense rank based on age per state
windowSpec = Window.partitionBy("state").orderBy(col("age").desc())
df_emp.withColumn("dense_rank_age", dense_rank().over(windowSpec)).show()

164. Row number by salary globally
windowSpec = Window.orderBy(col("salary").desc())
df_emp.withColumn("global_row_num", row_number().over(windowSpec)).show()

165. Rank by age globally
windowSpec = Window.orderBy(col("age").desc())
df_emp.withColumn("global_rank_age", rank().over(windowSpec)).show()

166. Lead and lag age globally
windowSpec = Window.orderBy("age")
df_emp.withColumn("prev_age", lag("age").over(windowSpec)).withColumn("next_age", lead("age").over(windowSpec)).show()

167. Employees with cumulative salary globally
windowSpec = Window.orderBy("salary").rowsBetween(Window.unboundedPreceding, Window.currentRow)
df_emp.withColumn("cum_salary_global", sum("salary").over(windowSpec)).show()

168. Top N salaries globally (top 3)
windowSpec = Window.orderBy(col("salary").desc())
df_emp.withColumn("rank", row_number().over(windowSpec)).filter(col("rank")<=3).show()

169. Employees with salary percent rank per department
from pyspark.sql.functions import percent_rank
windowSpec = Window.partitionBy("dept_name").orderBy(col("salary"))
df_emp.withColumn("percent_rank_salary", percent_rank().over(windowSpec)).show()

170. Employees with Ntile (quartiles) per department
from pyspark.sql.functions import ntile
windowSpec = Window.partitionBy("dept_name").orderBy("salary")
df_emp.withColumn("quartile", ntile(4).over(windowSpec)).show()

171. Employees whose rank = 1 per department (highest salary)
windowSpec = Window.partitionBy("dept_name").orderBy(col("salary").desc())
df_emp.withColumn("rank", row_number().over(windowSpec)).filter(col("rank")==1).show()

172. Employees whose dense_rank = 1 per state (oldest)
windowSpec = Window.partitionBy("state").orderBy(col("age").desc())
df_emp.withColumn("dense_rank", dense_rank().over(windowSpec)).filter(col("dense_rank")==1).show()

173. Employees with salary difference from max salary per department
from pyspark.sql.functions import max
windowSpec = Window.partitionBy("dept_name")
df_emp.withColumn("max_salary_dept", max("salary").over(windowSpec)).withColumn("salary_diff_max", col("max_salary_dept")-col("salary")).show()

174. Cumulative sum of age per state
windowSpec = Window.partitionBy("state").orderBy("age").rowsBetween(Window.unboundedPreceding, Window.currentRow)
df_emp.withColumn("cum_age_state", sum("age").over(windowSpec)).show()

175. Average salary per state
windowSpec = Window.partitionBy("state")
df_emp.withColumn("avg_salary_state", avg("salary").over(windowSpec)).show()

176. Row number per state ordered by age
windowSpec = Window.partitionBy("state").orderBy("age")
df_emp.withColumn("row_num_state", row_number().over(windowSpec)).show()

177. Rank per state by salary descending
windowSpec = Window.partitionBy("state").orderBy(col("salary").desc())
df_emp.withColumn("rank_state_salary", rank().over(windowSpec)).show()

178. Dense rank per state by salary descending
windowSpec = Window.partitionBy("state").orderBy(col("salary").desc())
df_emp.withColumn("dense_rank_state_salary", dense_rank().over(windowSpec)).show()

179. Lead salary per state by 2 rows
windowSpec = Window.partitionBy("state").orderBy("salary")
df_emp.withColumn("lead_salary_2", lead("salary",2).over(windowSpec)).show()

180. Lag salary per state by 2 rows
windowSpec = Window.partitionBy("state").orderBy("salary")
df_emp.withColumn("lag_salary_2", lag("salary",2).over(windowSpec)).show()

181. Employees with cumulative salary per state
windowSpec = Window.partitionBy("state").orderBy("salary").rowsBetween(Window.unboundedPreceding, Window.currentRow)
df_emp.withColumn("cum_salary_state", sum("salary").over(windowSpec)).show()

182. Employees with cumulative average salary per state
windowSpec = Window.partitionBy("state").orderBy("salary").rowsBetween(Window.unboundedPreceding, Window.currentRow)
df_emp.withColumn("cum_avg_salary_state", avg("salary").over(windowSpec)).show()

183. Employees with rank <= 2 per department
windowSpec = Window.partitionBy("dept_name").orderBy(col("salary").desc())
df_emp.withColumn("rank", row_number().over(windowSpec)).filter(col("rank")<=2).show()

184. Employees with dense rank <= 2 per department
windowSpec = Window.partitionBy("dept_name").orderBy(col("salary").desc())
df_emp.withColumn("dense_rank", dense_rank().over(windowSpec)).filter(col("dense_rank")<=2).show()

185. Employees with salary difference to next employee per state
windowSpec = Window.partitionBy("state").orderBy("salary")
df_emp.withColumn("next_salary", lead("salary").over(windowSpec)).withColumn("salary_diff_next", col("next_salary")-col("salary")).show()

186. Employees with salary difference to previous employee per state
windowSpec = Window.partitionBy("state").orderBy("salary")
df_emp.withColumn("prev_salary", lag("salary").over(windowSpec)).withColumn("salary_diff_prev", col("salary")-col("prev_salary")).show()

187. Employees with cumulative salary and row number per department
windowSpec = Window.partitionBy("dept_name").orderBy("salary").rowsBetween(Window.unboundedPreceding, Window.currentRow)
df_emp.withColumn("cum_salary", sum("salary").over(windowSpec)).withColumn("row_num", row_number().over(Window.partitionBy("dept_name").orderBy("salary"))).show()

188. Employees with salary percent rank per department
from pyspark.sql.functions import percent_rank
windowSpec = Window.partitionBy("dept_name").orderBy("salary")
df_emp.withColumn("percent_rank", percent_rank().over(windowSpec)).show()

189. Employees with quartile (ntile=4) per department
from pyspark.sql.functions import ntile
windowSpec = Window.partitionBy("dept_name").orderBy("salary")
df_emp.withColumn("quartile", ntile(4).over(windowSpec)).show()

190. Employees whose rank = 1 per state (highest salary)
windowSpec = Window.partitionBy("state").orderBy(col("salary").
