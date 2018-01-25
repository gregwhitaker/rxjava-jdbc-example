package com.github.gregwhitaker.rxjavajdbc.example;

import com.github.davidmoten.rx.jdbc.Database;
import com.github.gregwhitaker.rxjavajdbc.example.model.Department;
import com.github.gregwhitaker.rxjavajdbc.example.model.Employee;
import rx.Observable;

/**
 * Starts the rxjava-jdbc-example application.
 */
public class Main {

    public static void main(String... args) throws Exception {
        Database db = Database.from("jdbc:h2:./build/mydatabase", "sa", "sa");

        // Query that returns no employees
        System.out.println();
        System.out.println("Example: getNoEmployees");
        getNoEmployees(db)
                .subscribe(System.out::println);

        // Query that returns all employees
        System.out.println();
        System.out.println("Example: getAllEmployees");
        getAllEmployees(db)
                .subscribe(System.out::println);

        // Query that returns all manufacturing employees
        System.out.println();
        System.out.println("Example: getAllManufacturingEmployees");
        getAllManufacturingEmployees(db)
                .subscribe(System.out::println);

        // Query that returns Bob Smith
        System.out.println();
        System.out.println("Example: getEmployee");
        getEmployee(db)
                .subscribe(System.out::println);

        // Query that returns Bob Smith and uses automapping for the
        // returned Employee object
        System.out.println();
        System.out.println("Example: getEmployeeUsingAutomapping");
        getEmployeeUsingAutomapping(db)
                .subscribe(System.out::println);

        // Query that returns all departments and uses automapping on an interface
        // for the returned Department object
        System.out.println();
        System.out.println("Example: getAllDepartmentsWithInterfaceMapping");
        getAllDepartmentsWithInterfaceMapping(db)
                .subscribe(department -> {
                    System.out.println(String.format("Department: %s - %s", department.id(), department.name()));
                });

        // Query that returns all departments and uses automapping with an annotated
        // query on the returned Department object
        System.out.println();
        System.out.println("Example: getAllDepartmentsUsingAnnotatedQuery");
        getAllDepartmentsUsingAnnotatedQuery(db)
                .subscribe(department -> {
                    System.out.println(String.format("Department: %s - %s", department.id(), department.name()));
                });

        // Creates a new employee and then returns the employee information by
        // composing the insert and select statements
        System.out.println();
        System.out.println("Example: createNewEmployee");
        createNewEmployee(db)
                .subscribe(System.out::println);

        // Deletes an employee from the database and returns the number of rows deleted
        System.out.println();
        System.out.println("Example: deleteEmployee");
        deleteEmployee(db)
                .subscribe(count -> {
                    System.out.println(String.format("Deleted %s employees", count));
                });

        // Updates an employee's last name and then returns the employee information
        // by composing the update and select statements
        System.out.println();
        System.out.println("Example: updateEmployee");
        updateEmployee(db)
                .subscribe(System.out::println);

        // Updates all manufacturing employees to marketing employees in a transaction
        System.out.println();
        System.out.println("Example: updateEmployeesInTransaction");
        updateEmployeesInTransaction(db)
                .subscribe(System.out::println);
    }

    /**
     * Executes a query that returns no results.
     *
     * @param db database connection
     * @return an observable that emits no results
     */
    private static Observable<Employee> getNoEmployees(Database db) {
        String sql = "SELECT * FROM employee e JOIN department d ON e.department_id = d.department_id WHERE employee_firstname LIKE 'Barbara'";

        return db.select(sql)
                .get(rs -> {
                    Employee employee = new Employee();
                    employee.setId(rs.getInt("employee_id"));
                    employee.setFirstName(rs.getString("employee_firstname"));
                    employee.setLastName(rs.getString("employee_lastname"));
                    employee.setDepartment(rs.getString("department_name"));

                    return employee;
                });
    }

    /**
     * Executes a query that returns all employees in the database.
     *
     * @param db database connection
     * @return an observable that emits all employees
     */
    private static Observable<Employee> getAllEmployees(Database db) {
        String sql = "SELECT * FROM employee e JOIN department d ON e.department_id = d.department_id";

        return db.select(sql)
                .get(rs -> {
                    Employee employee = new Employee();
                    employee.setId(rs.getInt("employee_id"));
                    employee.setFirstName(rs.getString("employee_firstname"));
                    employee.setLastName(rs.getString("employee_lastname"));
                    employee.setDepartment(rs.getString("department_name"));

                    return employee;
                });
    }

    /**
     * Executes a query that returns all employees in the manufacturing department.
     *
     * @param db database connection
     * @return an observable that emits all employees in the manufacturing department
     */
    private static Observable<Employee> getAllManufacturingEmployees(Database db) {
        String sql = "SELECT employee_id, employee_firstname, employee_lastname, department_name FROM employee e " +
            "JOIN department d ON e.department_id = d.department_id " +
            "WHERE department_name = :department";

        return db.select(sql)
                .parameter("department", "Manufacturing")   // Example of named parameters
                .get(rs -> {
                    Employee employee = new Employee();
                    employee.setId(rs.getInt("employee_id"));
                    employee.setFirstName(rs.getString("employee_firstname"));
                    employee.setLastName(rs.getString("employee_lastname"));
                    employee.setDepartment(rs.getString("department_name"));

                    return employee;
                });
    }

    /**
     * Executes a query that returns the employee information for 'Bob Smith'.
     *
     * @param db database connection
     * @return an observable that emits a single employee, 'Bob Smith'
     */
    private static Observable<Employee> getEmployee(Database db) {
        String sql = "SELECT employee_id, employee_firstname, employee_lastname, department_name FROM employee e " +
                "JOIN department d ON e.department_id = d.department_id " +
                "WHERE employee_firstname = ? AND " +
                "employee_lastname = ?";

        return db.select(sql)
                .parameter("Bob")
                .parameter("Smith")
                .get(rs -> {
                    Employee employee = new Employee();
                    employee.setId(rs.getInt("employee_id"));
                    employee.setFirstName(rs.getString("employee_firstname"));
                    employee.setLastName(rs.getString("employee_lastname"));
                    employee.setDepartment(rs.getString("department_name"));

                    return employee;
                });
    }

    /**
     * Executes a query that returns the employee information for 'Bob Smith'.
     *
     * @param db database connection
     * @return an observable that emits a single employee, 'Bob Smith'
     */
    private static Observable<Employee> getEmployeeUsingAutomapping(Database db) {
        String sql = "SELECT employee_id, employee_firstname, employee_lastname, department_name FROM employee e " +
                "JOIN department d ON e.department_id = d.department_id " +
                "WHERE employee_firstname = ? AND " +
                "employee_lastname = ?";

        return db.select(sql)
                .parameter("Bob")
                .parameter("Smith")
                .autoMap(Employee.class);
    }

    /**
     * Executes a query that returns all departments in the database.
     *
     * @param db database connection
     * @return an observable that emits all departments
     */
    private static Observable<Department> getAllDepartmentsWithInterfaceMapping(Database db) {
        String sql = "SELECT * FROM department";

        return db.select(sql)
                .autoMap(Department.class);
    }

    /**
     * Executes a query that returns all departments in the database.
     *
     * @param db database connection
     * @return an observable that emits all departments
     */
    private static Observable<Department> getAllDepartmentsUsingAnnotatedQuery(Database db) {
        return db.select()
                .autoMap(Department.class);
    }

    /**
     * Executes an insert statement that creates a new employee in the database and then composes
     * a select statement to return the new employees information.
     *
     * @param db database connection
     * @return an observable that emits the newly created employee
     */
    private static Observable<Employee> createNewEmployee(Database db) {
        String createSql = "INSERT INTO employee (employee_firstname, employee_lastname, department_id) VALUES (?, ?, ?)";
        String selectSql = "SELECT employee_id, employee_firstname, employee_lastname, department_name FROM employee e " +
                "JOIN department d ON e.department_id = d.department_id " +
                "WHERE employee_id = ?";

        return db.update(createSql)                 // Create new employee
                        .parameter("Jerry")
                        .parameter("Cook")
                        .parameter(2)
                        .returnGeneratedKeys()
                        .getAs(Integer.class)
                .compose(db.select(selectSql)       // Query new employee details based on auto-incremented id assigned
                        .parameterTransformer()
                        .autoMap(Employee.class)
                );
    }

    /**
     * Executes a delete statement to remove employee 'Jerry Cook' from the database.
     *
     * @param db database connection
     * @return an observable that emits the number of rows removed from the database
     */
    private static Observable<Integer> deleteEmployee(Database db) {
        String sql = "DELETE FROM employee WHERE employee_firstname = ? AND employee_lastname = ?";

        return db.update(sql)
                .parameter("Jerry")
                .parameter("Cook")
                .count();
    }

    /**
     * Executes an update statement to update employee 2's last name and then return the
     * employee record from the database.
     *
     * @param db database connection
     * @return an observable that emits the newly updated employee record
     */
    private static Observable<Employee> updateEmployee(Database db) {
        String updateSql = "UPDATE employee SET employee_lastname = ? WHERE employee_id = ? ";
        String selectSql = "SELECT employee_id, employee_firstname, employee_lastname, department_name FROM employee e " +
                "JOIN department d ON e.department_id = d.department_id " +
                "WHERE employee_id = ?";

        return db.update(updateSql)
                        .parameters("Gray", 2)
                        .count()
                .compose(db.select(selectSql)
                        .parameter(2)
                        .dependsOnTransformer()
                        .autoMap(Employee.class)
                );
    }

    /**
     * Executes an update statement in a transaction to update all manufacturing employees to marketing employees
     * and then returns all employee records in the database.
     *
     * @param db database connection
     * @return an observable that emits all employees in the database
     */
    private static Observable<Employee> updateEmployeesInTransaction(Database db) {
        String updateSql = "UPDATE employee SET department_id = ? WHERE department_id = ?";
        String selectSql = "SELECT employee_id, employee_firstname, employee_lastname, department_name FROM employee e JOIN department d ON e.department_id = d.department_id";

        Observable<Boolean> begin = db.beginTransaction();

        // Updates all of the manufacturing employees to Marketing employees
        Observable<Integer> updates = db.update(updateSql)
                .dependsOn(begin)
                .parameters(2, 4)
                .count();

        Observable<Boolean> commit = db.commit(updates);

        return db.select(selectSql)
                .dependsOn(commit)
                .autoMap(Employee.class);
    }
}
