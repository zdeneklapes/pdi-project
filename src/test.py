import json
import os
import random
import subprocess
from pathlib import Path

import pytest

from lib.mylogger import MyLogger
from lib.program import Program


@pytest.fixture(scope="module")
def program():
    """Return the path to the main script."""
    return Program({}, MyLogger(["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]))


def run_task(task_id, setup_environment, bounding_box="-180 180 -180 180"):
    """Run the main script for a specific task."""
    args = [
        "python",
        "src/main.py",
        "--bounding-box", *bounding_box.split(),
        "--data_dir", str(setup_environment["data_dir"]),
        "--output_dir", str(setup_environment["output_dir"]),
        "--mode", "batch",
        "--task", str(task_id),
    ]
    result = subprocess.run(args, capture_output=True, text=True)
    return result


# Test for Task 1
def test_task_1(setup_environment, program):
    program.logger.debug("Running Task 1")
    result = run_task(1, setup_environment)
    assert result.returncode == 0, f"Task 1 failed: {result.stderr}"
    expected_output = (
        "id: vehicle_6 | vtype:  1 | ltype:  1 | lat: 26.0000 | lng: 16.0000 | bearing: 225.0 | lineid:  106 | routeid:    10 | lastupdate:   1700000700000 |".strip(),
        "id: vehicle_1 | vtype:  1 | ltype:  1 | lat: 20.0000 | lng: 10.0000 | bearing:  45.0 | lineid:  101 | routeid:     5 | lastupdate:   1700000000000 |".strip(),
        "id: vehicle_3 | vtype:  1 | ltype:  1 | lat: 22.5000 | lng: 12.5000 | bearing: 180.0 | lineid:  103 | routeid:     7 | lastupdate:   1700000300000 |".strip(),
        "id: vehicle_3 | vtype:  1 | ltype:  1 | lat: 23.0000 | lng: 13.0000 | bearing: 180.0 | lineid:  103 | routeid:     7 | lastupdate:   1700000400000 |".strip(),
        "id: vehicle_2 | vtype:  2 | ltype:  1 | lat: 25.0000 | lng: 15.0000 | bearing:  90.0 | lineid:  102 | routeid:     6 | lastupdate:   1700000100000 |".strip(),
        "id: vehicle_6 | vtype:  1 | ltype:  1 | lat: 28.0000 | lng: 18.0000 | bearing: 180.0 | lineid:  106 | routeid:    10 | lastupdate:   1700000800000 |".strip(),
        "id: vehicle_6 | vtype:  1 | ltype:  1 | lat: 32.0000 | lng: 22.0000 | bearing: 180.0 | lineid:  106 | routeid:    10 | lastupdate:   1700001000000 |".strip(),
        "id: vehicle_4 | vtype:  1 | ltype:  1 | lat: 24.0000 | lng: 14.0000 | bearing: 270.0 | lineid:  104 | routeid:     8 | lastupdate:   1700000500000 |".strip(),
        "id: vehicle_6 | vtype:  1 | ltype:  1 | lat: 30.0000 | lng: 20.0000 | bearing: 180.0 | lineid:  106 | routeid:    10 | lastupdate:   1700000900000 |".strip(),
        "id: vehicle_5 | vtype:  1 | ltype:  1 | lat: 25.5000 | lng: 15.5000 | bearing: 135.0 | lineid:  105 | routeid:     9 | lastupdate:   1700000600000 |".strip(),
        "id: vehicle_6 | vtype:  1 | ltype:  1 | lat: 34.0000 | lng: 24.0000 | bearing: 180.0 | lineid:  106 | routeid:    10 | lastupdate:   1700001100000 |".strip(),
        "id: vehicle_3 | vtype:  1 | ltype:  1 | lat: 22.0000 | lng: 12.0000 | bearing: 180.0 | lineid:  103 | routeid:     7 | lastupdate:   1700000200000 |".strip(),
    )
    for line in result.stdout.split("\n"):
        if len(line.strip()) == 0:
            print("Skipping empty line")
            continue
        assert line.strip() in expected_output, f"Unexpected line in Task 1 output: {line}"


# Test for Task 2
def test_task_2(setup_environment):
    """Test that Task 2 correctly identifies trolleybuses at their final stop."""
    result = run_task(2, setup_environment)
    assert result.returncode == 0, f"Task 2 failed: {result.stderr}"

    # Define expected output
    expected_output = [
        "id: vehicle_2 | laststopid:     8 | finalstopid:     8 |".strip(),
    ]

    # Parse and validate output
    output_lines = [line.strip() for line in result.stdout.split("\n") if line.strip()]
    for line in output_lines:
        assert line in expected_output, f"Unexpected line in Task 2 output: {line}"

    # Ensure all expected lines are in the output
    for expected_line in expected_output:
        assert expected_line in output_lines, f"Expected line not found in Task 2 output: {expected_line}"


# Test for Task 3
@pytest.mark.xfail(reason="Task 3 is implemented incorrectly.")
def test_task_3(setup_environment):
    """Test that Task 3 identifies delayed vehicles reducing delay, sorted by improvement."""
    result = run_task(3, setup_environment)
    assert result.returncode == 0, f"Task 3 failed: {result.stderr}"

    # Define expected output
    expected_output = [
        "ID: vehicle_3 | Improvement:       0.00 | Previous Delay:      80.00 | Current Delay:      80.00".strip(),
        "ID: vehicle_6 | Improvement:       0.00 | Previous Delay:     100.00 | Current Delay:     100.00".strip(),
        "ID: vehicle_6 | Improvement:      40.00 | Previous Delay:     100.00 | Current Delay:      60.00".strip(),
        "ID: vehicle_2 | Improvement:       0.00 | Previous Delay:      10.00 | Current Delay:      10.00".strip(),
        "ID: vehicle_3 | Improvement:       0.00 | Previous Delay:      80.00 | Current Delay:     100.00".strip(),
        "ID: vehicle_3 | Improvement:       0.00 | Previous Delay:      80.00 | Current Delay:     120.00".strip(),
        "ID: vehicle_6 | Improvement:     -20.00 | Previous Delay:      60.00 | Current Delay:      80.00".strip(),
        "ID: vehicle_6 | Improvement:     -20.00 | Previous Delay:      60.00 | Current Delay:     120.00".strip(),
        "ID: vehicle_5 | Improvement:       0.00 | Previous Delay:      50.00 | Current Delay:      50.00".strip(),
        "ID: vehicle_4 | Improvement:       0.00 | Previous Delay:     300.00 | Current Delay:     300.00".strip(),
        "ID: vehicle_6 | Improvement:      80.00 | Previous Delay:     120.00 | Current Delay:      40.00".strip(),
    ]

    # Parse and validate output
    output_lines = [line.strip() for line in result.stdout.split("\n") if line.strip()]
    for index, line in enumerate(output_lines):
        assert expected_output[index] == line, f"Unexpected line in Task 3 output: {line}"

    # # Ensure all expected lines are in the output
    # for expected_line in expected_output:
    #     assert expected_line in output_lines, f"Expected line not found in Task 3 output: {expected_line}"
    #
    # # Check sorting by improvement
    # improvements = [
    #     float(line.split("|")[1].split(":")[1].strip())
    #     for line in output_lines if "Improvement" in line
    # ]
    # assert improvements == sorted(improvements, reverse=True), "Task 3 output is not sorted by improvement."


# Test for Task 4
def test_task_4(setup_environment):
    result = run_task(4, setup_environment)
    assert result.returncode == 0, f"Task 4 failed: {result.stderr}"
    print(result.stdout)
    # output = validate_output(4, setup_environment)
    # assert "Min Delay" in output[0] and "Max Delay" in output[0], "Task 4 output is not formatted as expected."


# Test for Task 5
def test_task_5(setup_environment):
    result = run_task(5, setup_environment)
    assert result.returncode == 0, f"Task 5 failed: {result.stderr}"
    # output = validate_output(5, setup_environment)
    # assert "Min Interval" in output[0] and "Max Interval" in output[0], "Task 5 output is not formatted as expected."


# Clean up environment after tests
def test_cleanup(setup_environment):
    """Remove test environment after tests."""
    base_dir = setup_environment["base_dir"]
    for item in base_dir.iterdir():
        if item.is_file():
            item.unlink()
        elif item.is_dir():
            for subitem in item.iterdir():
                subitem.unlink()
            item.rmdir()
    base_dir.rmdir()
