package com.github.gregwhitaker.rxjavajdbc.example;

import com.github.davidmoten.rx.jdbc.Database;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Starts the rxjava-jdbc-example application.
 */
public class Main {
    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    public static void main(String... args) throws Exception {
        Database db = Database.from("jdbc:h2:./build/mydatabase", "sa", "sa");

        System.out.println();

        getAllEmployees(db);

        System.out.println();

        getAllManufacturingEmployees(db);
    }

    private static void getAllEmployees(Database db) {
        LOGGER.info("STARTING: getAllEmployees");

        String sql = "SELECT * FROM employee e JOIN department d ON e.department_id = d.department_id";

        List<Employee> employees = db.select(sql)
                .get(rs -> {
                    Employee employee = new Employee();
                    employee.setId(rs.getInt("employee_id"));
                    employee.setFirstName(rs.getString("employee_firstname"));
                    employee.setLastName(rs.getString("employee_lastname"));
                    employee.setDepartment(rs.getString("department_name"));

                    return employee;
                })
                .doOnNext(System.out::println)
                .toList()
                .toBlocking()
                .last();

        LOGGER.info("FINISHED: getAllEmployees");
    }

    private static void getAllManufacturingEmployees(Database db) {
        LOGGER.info("STARTING: getAllManufacturingEmployees");

        String sql = "SELECT EMPLOYEE_ID, EMPLOYEE_FIRSTNAME, EMPLOYEE_LASTNAME, DEPARTMENT_NAME FROM EMPLOYEE e " +
            "JOIN DEPARTMENT d ON e.DEPARTMENT_ID = d.DEPARTMENT_ID " +
            "WHERE DEPARTMENT_NAME = 'Manufacturing'";

        List<Employee> employees = db.select(sql)
                .get(rs -> {
                    Employee employee = new Employee();
                    employee.setId(rs.getInt("employee_id"));
                    employee.setFirstName(rs.getString("employee_firstname"));
                    employee.setLastName(rs.getString("employee_lastname"));
                    employee.setDepartment(rs.getString("department_name"));

                    return employee;
                })
                .doOnNext(System.out::println)
                .toList()
                .toBlocking()
                .last();

        LOGGER.info("FINISHED: getAllManufacturingEmployees");
    }
}
