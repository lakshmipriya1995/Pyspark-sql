import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

with open('./config.json') as config:
    inputPaths = json.load(config)

inputPathEmployee = inputPaths["employeeDetails"]
inputPathManager = inputPaths["manager"]
inputPathSalary = inputPaths["employee_salary"]
outputPath = inputPaths["Output_path"]

# Creating spark session
spark = SparkSession.builder.appName("EmployeeReport").master("local[*]").getOrCreate()

# Reading Employee csv file
employeeDF = spark.read.option("header", "true").csv(inputPathEmployee)
employeeDF.createOrReplaceTempView("employeeDF")

# Reading manager csv file
managerDetails = spark.read.option("header", "true").csv(inputPathManager)
managerDF = managerDetails.alias("managerDF")
managerDF.createOrReplaceTempView("managerDF")

# Reading salary csv file
salaryDF = spark.read.option("header", "true").csv(inputPathSalary)
salaryDF.createOrReplaceTempView("salaryDF")

# Calculating employee annual salary
employeeAnnualSalary = salaryDF.groupBy("Employee").agg(
    sum("MonthlySalary").cast("Integer").alias("EmployeeTotalSalary")).alias("a")
employeeAnnualSalaryDF = employeeAnnualSalary.join(managerDF, col("a.Employee") == col("managerDF.Employee"), 'inner') \
    .selectExpr("a.Employee", "a.EmployeeTotalSalary", "managerDF.ManagerName")
employeeAnnualSalaryDF.createOrReplaceTempView("employeeAnnualSalaryDF")

# Total Annual salary of all the employees under each manager
totalSalaryUnderManagerDF = spark.sql(
    """Select 
  a.ManagerName, 
  sum(b.EmployeeTotalSalary) as TotalSalary 
from 
  managerDF a 
  join employeeAnnualSalaryDF b on a.Employee = b.Employee 
group by 
  a.ManagerName
""")
totalSalaryUnderManagerDF.createOrReplaceTempView("totalSalaryUnderManagerDF")

# Final dataset to aggregate
finalDataset = spark.sql(
    """select
    a.ManagerName,
    a.TotalSalary,
    b.Employee,
    b.EmployeeTotalSalary,
    c.Month,
    c.MonthlySalary
  from
  totalSalaryUnderManagerDF a
    join employeeAnnualSalaryDF b on a.ManagerName = b.ManagerName
    join salaryDF c on b.Employee = c.Employee
  """)

finalDataset.createOrReplaceTempView("finalDataset")

# Final output report
EmployeeReport = spark.sql("""select 
  ManagerName, 
  max(TotalSalary) over(partition by ManagerName) as highestTotalSalary, 
  Employee, 
  EmployeeTotalSalary, 
  dense_rank() over(
    partition by ManagerName 
    order by 
      EmployeeTotalSalary desc
  ) as EmployeeRank, 
  Month, 
  MonthlySalary, 
  dense_rank() over(
    partition by Employee 
    order by 
      MonthlySalary desc
  ) as EmployeeSalaryRank 
from 
  finalDataset 
order by 
  highestTotalSalary desc, 
  EmployeeTotalSalary desc
""")

EmployeeReport.show(n=100, truncate=False)
EmployeeReport.write.mode("Overwrite").csv(outputPath, header='true')

