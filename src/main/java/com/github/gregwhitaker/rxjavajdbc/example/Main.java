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
        System.out.println("Example: getBobSmith");
        getBobSmith(db)
                .subscribe(System.out::println);

        // Query that returns Bob Smith and uses automapping for the
        // returned Employee object
        System.out.println();
        System.out.println("Example: getBobSmithWithMapping");
        getBobSmithWithMapping(db)
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
                .toBlocking()
                .subscribe(department -> {
                    System.out.println(String.format("Department: %s - %s", department.id(), department.name()));
                });
    }

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

    private static Observable<Employee> getAllManufacturingEmployees(Database db) {
        String sql = "SELECT EMPLOYEE_ID, EMPLOYEE_FIRSTNAME, EMPLOYEE_LASTNAME, DEPARTMENT_NAME FROM EMPLOYEE e " +
            "JOIN DEPARTMENT d ON e.DEPARTMENT_ID = d.DEPARTMENT_ID " +
            "WHERE DEPARTMENT_NAME = 'Manufacturing'";

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

    private static Observable<Employee> getBobSmith(Database db) {
        String sql = "SELECT EMPLOYEE_ID, EMPLOYEE_FIRSTNAME, EMPLOYEE_LASTNAME, DEPARTMENT_NAME FROM EMPLOYEE e " +
                "JOIN DEPARTMENT d ON e.DEPARTMENT_ID = d.DEPARTMENT_ID " +
                "WHERE EMPLOYEE_FIRSTNAME = 'Bob' AND " +
                "EMPLOYEE_LASTNAME = 'Smith'";

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

    private static Observable<Employee> getBobSmithWithMapping(Database db) {
        String sql = "SELECT EMPLOYEE_ID, EMPLOYEE_FIRSTNAME, EMPLOYEE_LASTNAME, DEPARTMENT_NAME FROM EMPLOYEE e " +
                "JOIN DEPARTMENT d ON e.DEPARTMENT_ID = d.DEPARTMENT_ID " +
                "WHERE EMPLOYEE_FIRSTNAME = 'Bob' AND " +
                "EMPLOYEE_LASTNAME = 'Smith'";

        return db.select(sql)
                .autoMap(Employee.class);
    }

    private static Observable<Department> getAllDepartmentsWithInterfaceMapping(Database db) {
        String sql = "SELECT * FROM department";

        return db.select(sql)
                .autoMap(Department.class);
    }

    private static Observable<Department> getAllDepartmentsUsingAnnotatedQuery(Database db) {
        return db.select()
                .autoMap(Department.class);
    }
}
