--Average salary by year
SELECT Year, AVG(TotalPayBenefits) AS avg_salary
FROM payroll_processed
GROUP BY Year
ORDER BY Year;

--Top 10 highest paid employees
SELECT EmployeeName, JobTitle, TotalPayBenefits
FROM payroll_processed
ORDER BY TotalPayBenefits DESC
LIMIT 10;
