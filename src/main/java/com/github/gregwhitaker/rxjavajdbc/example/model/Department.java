package com.github.gregwhitaker.rxjavajdbc.example.model;

import com.github.davidmoten.rx.jdbc.annotations.Column;

public interface Department {

    @Column("department_id")
    int id();

    @Column("department_name")
    String name();
}
