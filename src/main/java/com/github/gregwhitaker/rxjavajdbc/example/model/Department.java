package com.github.gregwhitaker.rxjavajdbc.example.model;

import com.github.davidmoten.rx.jdbc.annotations.Column;
import com.github.davidmoten.rx.jdbc.annotations.Query;

@Query("SELECT * FROM department")
public interface Department {

    @Column("department_id")
    int id();

    @Column("department_name")
    String name();
}
