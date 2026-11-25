package edu.sau.pro3;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;

@Repository
class PersonnelRepository {
    private static final String EMPLOYEE = """
            SELECT e.empno, e.ename, e.sal, e.comm, e.img,
                   manager.ename AS manager, d.dname AS department
            FROM emp e
            JOIN dept d ON d.deptno = e.deptno
            LEFT JOIN emp manager ON manager.empno = e.mgr
            """;
    private final JdbcClient jdbc;

    PersonnelRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    List<PersonnelName> list() {
        return jdbc.sql("SELECT empno, ename FROM emp ORDER BY ename")
                .query(PersonnelName.class).list();
    }

    List<Employee> directory() {
        return jdbc.sql(EMPLOYEE + "ORDER BY e.ename, e.empno").query(Employee.class).list();
    }

    Optional<Employee> find(int empno) {
        return jdbc.sql(EMPLOYEE + "WHERE e.empno = :empno")
                .param("empno", empno).query(Employee.class).optional();
    }

    record PersonnelName(int empno, String ename) {}

    record Employee(int empno, String ename, BigDecimal sal, BigDecimal comm, String img,
                    String manager, String department) {}
}
