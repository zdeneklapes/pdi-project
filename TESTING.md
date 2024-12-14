# TESTING

## Project PDI - Process Real-Time Public Transit Data Using Apache Spark/Flink

### Author: Zdenek Lapes, xlapes02

---

## Setup

Please follow the "**Install dependencies**" section inside [INSTALL.md](INSTALL.md) to install and activate virtual environment inside the container.

## Test TASK 1:

### Generate test data

```
python src/gen_test_data.py --task 2
```

### Process the data

```
python src/main.py --bounding-box -180 180 -180 180 --data_dir tmp/testing/data/1 --output_dir tmp/testing/output/ --mode batch --task 1
```

### Expected output

The order of the lines can be different, but the content should be the same, depending on the order of the data processing):

```
id:   1002 | vtype:  1 | ltype:  5 | lat: 49.2721 | lng: 16.7388 | bearing:  89.1 | lineid:   27 | routeid:  4587 | lastupdate:   1729962576745 |
id:   1003 | vtype:  2 | ltype:  3 | lat: 49.1194 | lng: 16.5571 | bearing: 147.4 | lineid:  187 | routeid:   807 | lastupdate:   1705117840384 |
id:   1002 | vtype:  1 | ltype:  1 | lat: 49.1050 | lng: 16.6918 | bearing:  49.4 | lineid:   27 | routeid:  4468 | lastupdate:   1718179140120 |
id:   1001 | vtype:  4 | ltype:  3 | lat: 49.1884 | lng: 16.6395 | bearing: 285.5 | lineid:  189 | routeid:   695 | lastupdate:   1705628208741 |
id:   1000 | vtype:  5 | ltype:  2 | lat: 49.2471 | lng: 16.5175 | bearing:  19.6 | lineid:  150 | routeid:  4120 | lastupdate:   1707951455696 |
id:   1000 | vtype:  0 | ltype:  1 | lat: 49.1182 | lng: 16.6529 | bearing: 225.5 | lineid:  175 | routeid:  4886 | lastupdate:   1731144586476 |
id:   1002 | vtype:  0 | ltype:  0 | lat: 49.1357 | lng: 16.7911 | bearing: 214.4 | lineid:   89 | routeid:  2570 | lastupdate:   1701094024844 |
id:   1002 | vtype:  4 | ltype:  5 | lat: 49.2086 | lng: 16.6315 | bearing: 189.9 | lineid:  171 | routeid:  2453 | lastupdate:   1730560533420 |
id:   1000 | vtype:  0 | ltype:  5 | lat: 49.1482 | lng: 16.5718 | bearing: 174.4 | lineid:   18 | routeid:  4364 | lastupdate:   1718318278723 |
id:   1000 | vtype:  3 | ltype:  2 | lat: 49.2711 | lng: 16.7956 | bearing: 162.7 | lineid:   94 | routeid:  3033 | lastupdate:   1702783290795 |
```

## Test TASK 2:

### Generate test data

```
python src/gen_test_data.py --task 2
```

### Process the data

```
python src/main.py --bounding-box -180 180 -180 180 --data_dir tmp/testing/data/2 --output_dir tmp/testing/output/ --mode batch --task 2
```

### Expected output

The order of the lines can be different, but the content should be the same, depending on the order of the data processing):

```
id:   1002 | laststopid:  2002 | finalstopid:  2002 |
id:   1001 | laststopid:  2001 | finalstopid:  2001 |
id:   1003 | laststopid:  2003 | finalstopid:  2003 |
```

## Test TASK 3:

This task was particularly challenging and required significant tweaking and polishing of the implementation. I implemented custom windowing logic to handle event processing. Specifically, the logic waits until at least 4 events are received from a single vehicle before processing all previous records for vehicles that have more than 2 records. During this processing, the improvement in delay is computed for each vehicle and the results are sorted in descending order of improvement.

One downside of this solution is that results are not visible immediately. Instead, they are emitted only after the 4th event for any vehicle is received. This delay arises from the custom windowing logic. For example, in a streaming mode scenario where records are received every 10 seconds, the first results may appear with a delay of up to 40 seconds.

Despite this limitation, I believe the solution is robust and can be easily extended to accommodate different window sizes. By modifying the constant in the code (e.g., setting it to 10 instead of 4), the window can be adjusted to process at least 10 elements per vehicle, further improving the handling of out-of-order events.

For the testing dataset, a window size of 4 was sufficient to produce accurate and consistent results.

### Generate test data

```
python src/gen_test_data.py --task 3
```

### Process the data

```
python src/main.py --bounding-box -180 180 -180 180 --data_dir tmp/testing/data/3 --output_dir tmp/testing/output/ --mode batch --task 3
```

### Expected output

```
ID: 3003 | improvement: 3.00 | previous Delay: 20.00 | current Delay: 17.00 | lastupdate: 2024-12-12 21:46:45
ID: 3002 | improvement: 2.00 | previous Delay: 20.00 | current Delay: 18.00 | lastupdate: 2024-12-12 21:46:45
ID: 3001 | improvement: 1.00 | previous Delay: 20.00 | current Delay: 19.00 | lastupdate: 2024-12-12 21:46:45
```

## Test TASK 4:

### Generate test data

```
python src/gen_test_data.py --task 4
```

### Process the data

```
python src/main.py --bounding-box -180 180 -180 180 --data_dir tmp/testing/data/4 --output_dir tmp/testing/output/ --mode batch --task 4
```

### Expected output

```
window start: 2024-12-12 21:44:00 | window end: 2024-12-12 21:47:00 | elements: 1 | min delay:  1.00 | max delay:  1.00 |
window start: 2024-12-12 21:44:10 | window end: 2024-12-12 21:47:10 | elements: 2 | min delay:  1.00 | max delay:  2.00 |
window start: 2024-12-12 21:44:20 | window end: 2024-12-12 21:47:20 | elements: 3 | min delay:  1.00 | max delay:  3.00 |
window start: 2024-12-12 21:44:30 | window end: 2024-12-12 21:47:30 | elements: 4 | min delay:  1.00 | max delay:  4.00 |
window start: 2024-12-12 21:44:40 | window end: 2024-12-12 21:47:40 | elements: 5 | min delay:  1.00 | max delay:  5.00 |
window start: 2024-12-12 21:44:50 | window end: 2024-12-12 21:47:50 | elements: 6 | min delay:  1.00 | max delay:  6.00 |
window start: 2024-12-12 21:45:00 | window end: 2024-12-12 21:48:00 | elements: 7 | min delay:  1.00 | max delay:  7.00 |
window start: 2024-12-12 21:45:10 | window end: 2024-12-12 21:48:10 | elements: 8 | min delay:  1.00 | max delay:  8.00 |
window start: 2024-12-12 21:45:20 | window end: 2024-12-12 21:48:20 | elements: 9 | min delay:  1.00 | max delay:  9.00 |
window start: 2024-12-12 21:45:30 | window end: 2024-12-12 21:48:30 | elements: 10 | min delay:  1.00 | max delay: 10.00 |
window start: 2024-12-12 21:45:40 | window end: 2024-12-12 21:48:40 | elements: 11 | min delay:  1.00 | max delay: 11.00 |
window start: 2024-12-12 21:45:50 | window end: 2024-12-12 21:48:50 | elements: 12 | min delay:  1.00 | max delay: 12.00 |
window start: 2024-12-12 21:46:00 | window end: 2024-12-12 21:49:00 | elements: 13 | min delay:  1.00 | max delay: 13.00 |
window start: 2024-12-12 21:46:10 | window end: 2024-12-12 21:49:10 | elements: 14 | min delay:  1.00 | max delay: 14.00 |
window start: 2024-12-12 21:46:20 | window end: 2024-12-12 21:49:20 | elements: 15 | min delay:  1.00 | max delay: 15.00 |
window start: 2024-12-12 21:46:30 | window end: 2024-12-12 21:49:30 | elements: 16 | min delay:  1.00 | max delay: 16.00 |
window start: 2024-12-12 21:46:40 | window end: 2024-12-12 21:49:40 | elements: 17 | min delay:  1.00 | max delay: 17.00 |
window start: 2024-12-12 21:46:50 | window end: 2024-12-12 21:49:50 | elements: 18 | min delay:  1.00 | max delay: 18.00 |
window start: 2024-12-12 21:47:00 | window end: 2024-12-12 21:50:00 | elements: 18 | min delay:  2.00 | max delay: 19.00 |
window start: 2024-12-12 21:47:10 | window end: 2024-12-12 21:50:10 | elements: 18 | min delay:  3.00 | max delay: 20.00 |
window start: 2024-12-12 21:47:20 | window end: 2024-12-12 21:50:20 | elements: 18 | min delay:  4.00 | max delay: 21.00 |
window start: 2024-12-12 21:47:30 | window end: 2024-12-12 21:50:30 | elements: 18 | min delay:  5.00 | max delay: 22.00 |
window start: 2024-12-12 21:47:40 | window end: 2024-12-12 21:50:40 | elements: 18 | min delay:  6.00 | max delay: 23.00 |
window start: 2024-12-12 21:47:50 | window end: 2024-12-12 21:50:50 | elements: 17 | min delay:  7.00 | max delay: 23.00 |
window start: 2024-12-12 21:48:00 | window end: 2024-12-12 21:51:00 | elements: 16 | min delay:  8.00 | max delay: 23.00 |
window start: 2024-12-12 21:48:10 | window end: 2024-12-12 21:51:10 | elements: 15 | min delay:  9.00 | max delay: 23.00 |
window start: 2024-12-12 21:48:20 | window end: 2024-12-12 21:51:20 | elements: 14 | min delay: 10.00 | max delay: 23.00 |
window start: 2024-12-12 21:48:30 | window end: 2024-12-12 21:51:30 | elements: 13 | min delay: 11.00 | max delay: 23.00 |
window start: 2024-12-12 21:48:40 | window end: 2024-12-12 21:51:40 | elements: 12 | min delay: 12.00 | max delay: 23.00 |
window start: 2024-12-12 21:48:50 | window end: 2024-12-12 21:51:50 | elements: 11 | min delay: 13.00 | max delay: 23.00 |
window start: 2024-12-12 21:49:00 | window end: 2024-12-12 21:52:00 | elements: 10 | min delay: 14.00 | max delay: 23.00 |
window start: 2024-12-12 21:49:10 | window end: 2024-12-12 21:52:10 | elements: 9 | min delay: 15.00 | max delay: 23.00 |
window start: 2024-12-12 21:49:20 | window end: 2024-12-12 21:52:20 | elements: 8 | min delay: 16.00 | max delay: 23.00 |
window start: 2024-12-12 21:49:30 | window end: 2024-12-12 21:52:30 | elements: 7 | min delay: 17.00 | max delay: 23.00 |
window start: 2024-12-12 21:49:40 | window end: 2024-12-12 21:52:40 | elements: 6 | min delay: 18.00 | max delay: 23.00 |
window start: 2024-12-12 21:49:50 | window end: 2024-12-12 21:52:50 | elements: 5 | min delay: 19.00 | max delay: 23.00 |
window start: 2024-12-12 21:50:00 | window end: 2024-12-12 21:53:00 | elements: 4 | min delay: 20.00 | max delay: 23.00 |
window start: 2024-12-12 21:50:10 | window end: 2024-12-12 21:53:10 | elements: 3 | min delay: 21.00 | max delay: 23.00 |
window start: 2024-12-12 21:50:20 | window end: 2024-12-12 21:53:20 | elements: 2 | min delay: 22.00 | max delay: 23.00 |
window start: 2024-12-12 21:50:30 | window end: 2024-12-12 21:53:30 | elements: 1 | min delay: 23.00 | max delay: 23.00 |
```


## Test TASK 5:

### Generate test data

```
python src/gen_test_data.py --task 5
```

### Process the data

```
python src/main.py --bounding-box -180 180 -180 180 --data_dir tmp/testing/data/1 --output_dir tmp/testing/output/ --mode batch --task 1
```

### Expected output

The order of the lines can be different, but the content should be the same, depending on the order of the data processing):

```
ID:   5003 | From: 2024-12-12 21:46:50 | To: 2024-12-12 21:47:30 | Min Interval: 10.00 s | Max Interval: 10.00 s
ID:   5002 | From: 2024-12-12 21:46:50 | To: 2024-12-12 21:47:10 | Min Interval: 10.00 s | Max Interval: 10.00 s
ID:   5001 | From: 2024-12-12 21:46:50 | To: 2024-12-12 21:46:40 | Min Interval: 10.00 s | Max Interval: 10.00 s
```





