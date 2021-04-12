#!/bin/bash

function get_value_from_python_file() {
    VALUE=$(head -n 1 $1.py)
    VALUE=${VALUE#"$1 = "}
    VALUE=${VALUE:1:-1}
    echo $VALUE
}
